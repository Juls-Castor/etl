import os, pdb
import uuid
import logging
from datetime import datetime

import pandas as pd
import mysql.connector
from mysql.connector import Error

# =========================
# ENV VARS
# =========================
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_NAME = os.environ.get("MYSQL_DATABASE")
DB_USER = os.environ.get("MYSQL_USER")
DB_PASS = os.environ.get("MYSQL_PASSWORD")
INPUT_DIR = os.environ.get("INPUT_DIR", "./data")
LOG_DIR = os.environ.get("LOG_DIR", "/etl/logs")
ALLOW_MISSING_INVOICE_ID = (
    os.getenv("ALLOW_MISSING_INVOICE_ID", "false").strip().lower() == "true"
)
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR, f"load_and_clean_{datetime.now().strftime('%Y%m%d')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


# =========================
# DB CONNECTION
# =========================
def get_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )


# =========================
# LOAD DIMENSIONS
# =========================
def load_dimensions(conn):
    dims = {}
    queries = {
        "customer": "SELECT customer_id, customer_key FROM dim_customer",
        "item": "SELECT item_description, item_key FROM dim_item",
        "status": "SELECT status_name, status_key FROM dim_status",
    }

    for dim, query in queries.items():
        df = pd.read_sql(query, conn)
        dims[dim] = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))

    logger.info("Dimensions loaded into memory")
    return dims


# =========================
# LOAD STAGING
# =========================
def load_staging(conn, df, batch_id, file_name):
    df_stg = df.copy()
    df_stg["batch_id"] = batch_id
    df_stg["file_name"] = file_name

    cols = [
        "invoice_id",
        "issue_date",
        "customer_id",
        "customer_name",
        "item_description",
        "qty",
        "unit_price",
        "total",
        "status",
        "batch_id",
        "file_name",
    ]

    cursor = conn.cursor()
    insert_sql = f"""
        INSERT INTO stg_invoices ({",".join(cols)})
        VALUES ({",".join(["%s"] * len(cols))})
    """

    df_stg = df_stg.astype(object)
    df_stg = df_stg.where(pd.notnull(df_stg), None)

    cursor.executemany(insert_sql, df_stg[cols].values.tolist())
    conn.commit()
    cursor.close()

    logger.info(f"Staging loaded: {len(df_stg)} rows")


# =========================
# CLEAN & TRANSFORM
# =========================
def parse_mixed_date(s):
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%m/%d/%Y",
        #"%Y-%d-%m",
        #"%Y/%d/%m",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def clean_and_transform(df, dims):
    rejected = []

    # Normalize
    df["status"] = df["status"].str.upper().str.strip()
    df["customer_id"] = df["customer_id"].str.strip()
    df["item_description"] = df["item_description"].str.strip()

    # Date parsing
    df["issue_date_parsed"] = df["issue_date"].apply(parse_mixed_date)

    df = df.astype(object).where(pd.notnull(df), None)

    # Numeric parsing
    for col in ["qty", "unit_price", "total"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.upper()
            .str.replace(r"USD", "", regex=True)
            .str.replace(r"\$", "", regex=True)
            .str.replace(r"\s+", "", regex=True)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    valid_rows = []

    for _, row in df.iterrows():
        reason = None

        if pd.isna(row["invoice_id"]):
            if ALLOW_MISSING_INVOICE_ID:
                row["invoice_id"] = f"GEN-{uuid.uuid4()}"
                logger.warning(f"Generated invoice_id for row: {row.to_dict()}")
            else:
                reason = "UNKNOWN_INVOICE_ID"
        elif pd.isna(row["issue_date_parsed"]):
            reason = "INVALID_DATE"
        elif row["customer_id"] not in dims["customer"]:
            reason = "UNKNOWN_CUSTOMER"
        elif row["item_description"] not in dims["item"]:
            reason = "UNKNOWN_ITEM"
        elif row["status"] not in dims["status"]:
            reason = "UNKNOWN_STATUS"
        elif pd.isna(row["qty"]) or row["qty"] <= 0:
            reason = "INVALID_QUANTITY"
        elif pd.isna(row["unit_price"]) or row["unit_price"] < 0:
            reason = "INVALID_UNIT_PRICE"
        elif pd.isna(row["total"]) or row["total"] < 0:
            reason = "INVALID_TOTAL"

        elif round(row["qty"] * row["unit_price"], 2) != round(row["total"], 2):
            reason = "TOTAL_MISMATCH"

        if reason:
            rejected.append((row["invoice_id"], reason, row.to_json()))
        else:
            valid_rows.append(row)

    df_valid = pd.DataFrame(valid_rows)

    return df_valid, rejected


# =========================
# LOAD FACT
# =========================
def load_fact(conn, df, dims):
    if df.empty:
        logger.warning("No valid records to load into fact table")
        return

    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO fact_invoices (
            invoice_id, issue_date,
            customer_key, item_key, status_key,
            qty, unit_price, total
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE invoice_id = invoice_id
    """

    data = []
    for _, row in df.iterrows():
        data.append(
            (
                row["invoice_id"],
                row["issue_date_parsed"],
                dims["customer"][row["customer_id"]],
                dims["item"][row["item_description"]],
                dims["status"][row["status"]],
                int(row["qty"]),
                float(row["unit_price"]),
                float(row["total"]),
            )
        )

    cursor.executemany(insert_sql, data)
    total_inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    logger.info(f"Rows affected: {total_inserted} rows")


# =========================
# LOAD REJECTED
# =========================
def load_rejected(conn, rejected, batch_id, file_name):

    if not rejected:
        return

    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO rejected_invoices
        (invoice_id, reason, raw_record, batch_id, file_name)
        VALUES (%s,%s,%s,%s,%s)
    """

    data = [(inv, reason, raw, batch_id, file_name) for inv, reason, raw in rejected]

    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()

    logger.warning(f"Rejected records: {len(data)}")


# =========================
# MAIN
# =========================
def main():
    logger.info("ETL job started")

    conn = get_connection()
    dims = load_dimensions(conn)

    for file in os.listdir(INPUT_DIR):

        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(INPUT_DIR, file)
        batch_id = str(uuid.uuid4())

        logger.info(f"Processing file {file} | batch_id={batch_id}")

        df = pd.read_csv(file_path)
        load_staging(conn, df, batch_id, file)
        df_valid, rejected = clean_and_transform(df, dims)
        load_fact(conn, df_valid, dims)
        load_rejected(conn, rejected, batch_id, file)

    conn.close()
    logger.info("ETL job finished")


if __name__ == "__main__":
    main()
