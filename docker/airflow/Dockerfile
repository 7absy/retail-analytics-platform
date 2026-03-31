FROM apache/airflow:2.8.4-python3.10

COPY requirements.txt .

RUN pip install --no-cache-dir \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.10.txt" \
  -r requirements.txt

USER airflow