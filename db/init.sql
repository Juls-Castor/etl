USE data_eng;

-- =========================
-- 1. STAGING TABLE
-- =========================
DROP TABLE IF EXISTS stg_invoices;

CREATE TABLE stg_invoices (
    invoice_id       VARCHAR(50),
    issue_date        VARCHAR(50),
    customer_id       VARCHAR(50),
    customer_name     VARCHAR(100),
    item_description  VARCHAR(100),
    qty               VARCHAR(50),
    unit_price        VARCHAR(50),
    total             VARCHAR(50),
    status             VARCHAR(50),

    batch_id   VARCHAR(50),
    file_name  VARCHAR(255),
    load_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

-- =========================
-- 2. DIMENSIONS
-- =========================

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id  VARCHAR(50) UNIQUE,
    customer_name VARCHAR(100)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_customer (customer_id, customer_name) VALUES
('C-100','Acme Corp'),
('C-101','Globex Inc'),
('C-102','Soylent Corp'),
('C-103','Initech'),
('C-104','Umbrella Corp'),
('C-105','Stark Ind'),
('C-106','Wayne Ent'),
('C-107','Cyberdyne'),
('C-108','Massive Dynamic'),
('C-109','Hooli'),
('C-110','Dharma Initiative');


-- -------------------------

DROP TABLE IF EXISTS dim_item;
CREATE TABLE dim_item (
    item_key INT AUTO_INCREMENT PRIMARY KEY,
    item_description VARCHAR(100) UNIQUE
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_item (item_description) VALUES
('Server Setup'),
('Audit Service'),
('Licencia Software'),
('Front-end dev'),
('Consultoria Data'),
('API Access'),
('Cloud Storage'),
('Mantenimiento');

-- -------------------------

DROP TABLE IF EXISTS dim_status;
CREATE TABLE dim_status (
    status_key INT AUTO_INCREMENT PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_status (status_name) VALUES
('PROCESSING'),
('REFUNDED'),
('CANCELLED'),
('PENDING'),
('PAID');

-- =========================
-- 3. FACT TABLE
-- =========================

DROP TABLE IF EXISTS fact_invoices;
CREATE TABLE fact_invoices (
    invoice_id  VARCHAR(50) PRIMARY KEY,
    issue_date  DATE,

    customer_key INT,
    item_key     INT,
    status_key   INT,

    qty         INT,
    unit_price  DECIMAL(12,2),
    total       DECIMAL(14,2),

    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (item_key)     REFERENCES dim_item(item_key),
    FOREIGN KEY (status_key)   REFERENCES dim_status(status_key)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_fact_issue_date ON fact_invoices(issue_date);
CREATE INDEX idx_fact_customer  ON fact_invoices(customer_key);
CREATE INDEX idx_fact_status    ON fact_invoices(status_key);

-- =========================
-- 4. REJECTED RECORDS
-- =========================
DROP TABLE IF EXISTS rejected_invoices;

CREATE TABLE rejected_invoices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    invoice_id TEXT,
    reason TEXT,
    raw_record JSON,
    batch_id TEXT,
    file_name TEXT,
    rejected_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;

