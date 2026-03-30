FROM apache/airflow:3.1.7
USER airflow
RUN pip install uv
RUN uv pip install dbt-core dbt-snowflake

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PATH="/home/airflow/.local/bin:$PATH"