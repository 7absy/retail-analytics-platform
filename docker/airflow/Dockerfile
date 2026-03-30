FROM apache/airflow:2.8.4

USER root

# Copy requirements first (layer caching)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

USER airflow