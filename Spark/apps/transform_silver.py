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

ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])

CATALOG_URL = "http://lakekeeper:8181/catalog"
WAREHOUSE = "silver"
APP_CLIENT_ID = "spark-silver"
KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)

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
    # Catalog silver
    "spark.sql.catalog.lake_bronze": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_bronze.type": "rest",
    "spark.sql.catalog.lake_bronze.uri": CATALOG_URL,
    "spark.sql.catalog.lake_bronze.credential": f"{APP_CLIENT_ID}:{app_client_secret}",
    "spark.sql.catalog.lake_bronze.warehouse": "bronze",
    "spark.sql.catalog.lake_bronze.scope": "lakekeeper",
    "spark.sql.catalog.lake_bronze.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
    # Catalog gold
    "spark.sql.catalog.lake_silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_silver.type": "rest",
    "spark.sql.catalog.lake_silver.uri": CATALOG_URL,
    "spark.sql.catalog.lake_silver.credential": f"{APP_CLIENT_ID}:{app_client_secret}",
    "spark.sql.catalog.lake_silver.warehouse": "silver",
    "spark.sql.catalog.lake_silver.scope": "lakekeeper",
    "spark.sql.catalog.lake_silver.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
    # Common configurations
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.jars.packages": ",".join(spark_jars_packages),
}

spark_conf = SparkConf().setAppName("Bronze-to-Silver")
for k, v in config.items():
    spark_conf = spark_conf.set(k, v)

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# ===============================
# Read from bronze
# ===============================
try:
    logger.info("🔎 Read data from lake_bronze.bronze.business_stats ...")
    df = spark.sql("SELECT * FROM lake_bronze.bronze.business_stats")
    logger.info(f"Schema:")
    df.printSchema()
    count = df.count()
    logger.info(f"Read {count} rows from bronze")

    df.createOrReplaceTempView("bronze_stats")
except Exception as e:
    logger.error(f"❌ Error reading bronze: {e}")
    raise

# ===============================
# Transform
# ===============================
try:
    transformed = spark.sql(
        """
        SELECT
            Series_reference,
            to_date(concat(Period, '-01'), 'yyyy.MM-dd') as period_date,
            Data_value,
            STATUS,
            UNITS,
            Magnitude,
            Subject,
            `Group`,
            Series_title_1,
            Series_title_2,
            Series_title_3,
            Series_title_4,
            Series_title_5
        FROM bronze_stats
        WHERE Data_value IS NOT NULL
        """
    )
    logger.info("✅ Transform query compiled successfully")

    # Debug: only show sample to avoid crash executor
    logger.info("🔎 Schema after transform:")
    transformed.printSchema()

    logger.info("🔎 Sample 5 rows:")
    transformed.limit(5).show()

    # Debug: log query plan
    logger.info("🔎 Query plan:")
    transformed.explain(True)

    transformed.createOrReplaceTempView("silver_stats")
except Exception as e:
    logger.error(f"❌ Error transforming: {e}", exc_info=True)
    raise

# ===============================
# Write to silver
# ===============================
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake_silver.silver")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lake_silver.silver.business_stats_clean
        USING iceberg   
        TBLPROPERTIES ('format-version'='2')
        AS SELECT * FROM silver_stats
        """
    )
    logger.info("✅ Transform from bronze to silver successfully")
except Exception as e:
    logger.error(f"❌ Error writing to silver: {e}")
    raise
