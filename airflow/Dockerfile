FROM apache/airflow:3.4.0-python3.12

# The official image already has the Amazon provider; we add pyarrow for
# reading Parquet footers, pinned with Airflow's constraints file.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
