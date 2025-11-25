"""Spark job to promote NYC taxi data from NiFi bronze (s3a://bronze/raw/nyc_taxi)
to the Iceberg silver table lakekeeper.silver.nyc_taxi via Lakekeeper catalog.
"""

import argparse
import logging
from datetime import datetime

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp
from utils.gen_client import register_spark_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
ICEBERG_VERSION = "1.6.1"

CATALOG_URL = "http://lakekeeper:8181/catalog"
WAREHOUSE = "silver"
APP_CLIENT_ID = "spark"
APP_CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"
KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)

BRONZE_BUCKET = "s3a://bronze"
BRONZE_PREFIX = "raw/nyc_taxi"
ICEBERG_TABLE = "lakekeeper.silver.nyc_taxi"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load bronze NYC taxi parquet into Iceberg silver table"
    )
    parser.add_argument(
        "--process-date",
        dest="process_date",
        help="Date partition (YYYYMMDD) to ingest from bronze",
        default=datetime.utcnow().strftime("%Y%m%d"),
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    spark_jars_packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    )

    config = {
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
        "spark.sql.catalog.lakekeeper.credential": f"{APP_CLIENT_ID}:{APP_CLIENT_SECRET}",
        "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
        "spark.jars.packages": ",".join(spark_jars_packages),
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
    }

    spark_config = SparkConf().setMaster("local").setAppName("nyc-taxi-silver")
    for k, v in config.items():
        spark_config = spark_config.set(k, v)

    return SparkSession.builder.config(conf=spark_config).getOrCreate()


def read_bronze(spark: SparkSession, process_date: str):
    bronze_path = f"{BRONZE_BUCKET}/{BRONZE_PREFIX}/{process_date}/*.parquet"
    logger.info("Reading bronze data from %s", bronze_path)
    df = spark.read.parquet(bronze_path)
    logger.info("Read %s rows for date %s", df.count(), process_date)
    return df


def transform(df):
    logger.info("Transforming bronze dataframe")
    transformed = (
        df.select(
            col("vendorid").cast("int").alias("vendor_id"),
            to_timestamp(col("tpep_pickup_datetime")).alias("pickup_ts"),
            to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_ts"),
            col("passenger_count").cast("double"),
            col("trip_distance").cast("double"),
            col("ratecodeid").cast("int"),
            col("store_and_fwd_flag"),
            col("pulocationid").cast("int"),
            col("dolocationid").cast("int"),
            col("payment_type").cast("int"),
            col("fare_amount").cast("double"),
            col("extra").cast("double"),
            col("mta_tax").cast("double"),
            col("tip_amount").cast("double"),
            col("tolls_amount").cast("double"),
            col("improvement_surcharge").cast("double"),
            col("total_amount").cast("double"),
            col("congestion_surcharge").cast("double"),
        )
        .withColumn("pickup_date", to_date(col("pickup_ts")))
        .withColumn("ingested_at", current_timestamp())
    )
    return transformed


def write_silver(df):
    logger.info("Writing dataframe to Iceberg table %s", ICEBERG_TABLE)
    df.createOrReplaceTempView("silver_nyc_taxi")
    spark = df.sparkSession

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakekeeper.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakekeeper.silver.nyc_taxi (
            vendor_id INT,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            ratecodeid INT,
            store_and_fwd_flag STRING,
            pulocationid INT,
            dolocationid INT,
            payment_type INT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            pickup_date DATE,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (pickup_date)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    (df.writeTo(ICEBERG_TABLE).append())
    logger.info("Successfully wrote data to %s", ICEBERG_TABLE)


def main():
    args = parse_args()
    logger.info("Starting silver load for process date %s", args.process_date)

    spark = build_spark_session()

    bronze_df = read_bronze(spark, args.process_date)
    transformed_df = transform(bronze_df)
    write_silver(transformed_df)

    logger.info("Completed silver load for date %s", args.process_date)


if __name__ == "__main__":
    main()
