FROM apache/airflow:3.1.0-python3.11

# dbt lives in its own virtualenv so its dependencies never conflict with Airflow's
USER root
RUN mkdir -p /opt/dbt_venv && chown airflow:0 /opt/dbt_venv
USER airflow

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt_venv \
 && PIP_USER=false /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && /opt/dbt_venv/bin/python -c "import duckdb; duckdb.sql('INSTALL excel')"