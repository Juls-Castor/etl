FROM python:3.11-slim
RUN mkdir -p /etl/data /etl/logs /etl/scripts

WORKDIR /etl/scripts

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_and_clean.py .
CMD ["python", "load_and_clean.py"]