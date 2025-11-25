"""
DAG to orchestrate the NYC taxi bronze ➜ silver promotion.

Steps:
1. Wait for the upstream `csv_ingest_pipeline` DAG to finish triggering NiFi.
2. Submit the Spark job (`spark/apps/load_silver.py`) on the Spark cluster to load
the bronze parquet outputs (s3a://bronze/raw/nyc_taxi/<date>/...) into the Iceberg
table `lakekeeper.silver.nyc_taxi` via Lakekeeper.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from airflow import DAG

DEFAULT_PROCESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('process_date', ds_nodash) }}"
)

with DAG(
    dag_id="nyc_taxi_silver",
    description="Promote NYC Taxi bronze data to Iceberg silver via Spark",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["spark", "iceberg", "nyc_taxi"],
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id="run_silver_spark_job",
        conn_id="spark_default",
        application="/opt/airflow/spark-apps/load_silver.py",
        application_args=[
            "--process-date",
            DEFAULT_PROCESS_DATE_TEMPLATE,
        ],
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf={
            "spark.submit.deployMode": "client",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
    )

    run_spark_job
