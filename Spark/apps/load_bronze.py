import json
import logging

import pandas as pd
import pyspark
import requests
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from utils.gen_client import register_spark_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
ICEBERG_VERSION = "1.6.1"

CATALOG_URL = "http://lakekeeper:8181/catalog"
WAREHOUSE = "bronze"
APP_CLIENT_ID = "spark-bronze-7"
KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)

# Print spark version
logger.info(f"SPARK_VERSION: {SPARK_VERSION}")
logger.info(f"SPARK_MINOR_VERSION: {SPARK_MINOR_VERSION}")
logger.info(f"ICEBERG_VERSION: {ICEBERG_VERSION}")

# Register spark client in lakekeeper
app_client_id, app_client_secret = register_spark_client(APP_CLIENT_ID)

spark_jars_packages = (
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
)

config = {
    # Catalog configuration
    "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakekeeper.type": "rest",
    "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
    "spark.sql.catalog.lakekeeper.credential": f"{APP_CLIENT_ID}:{app_client_secret}",
    "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
    "spark.jars.packages": ",".join(spark_jars_packages),
    "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
}

spark_config = SparkConf().setMaster("local").setAppName("Iceberg-REST")
for k, v in config.items():
    spark_config = spark_config.set(k, v)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
logger.info("=========================================================")
logger.info("==============Read data from source===============")
logger.info("=========================================================")
source_path = "/opt/spark/data/business_data.csv"

df = spark.read.option("header", "true").csv(source_path)
logger.info(f"Read {df.count()} rows from {source_path}")
df.printSchema()

table_name = "lakekeeper.bronze.business_data"

# Register dataframe source as temp view
df.createOrReplaceTempView("source_stats")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakekeeper.bronze")
logger.info("=========================================================")
logger.info("Create namespace lakekeeper.bronze successfully")
logger.info("=========================================================")

# Remove table if exists
spark.sql("DROP TABLE IF EXISTS lakekeeper.bronze.business_stats")

# Create table from source data
spark.sql(
    """
    CREATE TABLE lakekeeper.bronze.business_stats
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
    AS SELECT * FROM source_stats
    """
)

logger.info("=========================================================")
logger.info("Created table lakekeeper.bronze.business_stats successfully")
logger.info("=========================================================")

# Append data to table
spark.sql(
    """
    INSERT INTO lakekeeper.bronze.business_stats
    SELECT * FROM source_stats
    """
)

spark.sql(
    """
    SELECT * FROM lakekeeper.bronze.business_stats
    """
).show(10, truncate=False)


logger.info("Load data to bronze table successfully")
