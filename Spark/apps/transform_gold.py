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
WAREHOUSE = "gold"
APP_CLIENT_ID = "spark-gold"
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
    # Catalog cho bronze
    "spark.sql.catalog.lake_silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_silver.type": "rest",
    "spark.sql.catalog.lake_silver.uri": CATALOG_URL,
    "spark.sql.catalog.lake_silver.credential": f"{APP_CLIENT_ID}:{app_client_secret}",
    "spark.sql.catalog.lake_silver.warehouse": "silver",
    "spark.sql.catalog.lake_silver.scope": "lakekeeper",
    "spark.sql.catalog.lake_silver.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
    # Catalog cho silver
    "spark.sql.catalog.lake_gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_gold.type": "rest",
    "spark.sql.catalog.lake_gold.uri": CATALOG_URL,
    "spark.sql.catalog.lake_gold.credential": f"{APP_CLIENT_ID}:{app_client_secret}",
    "spark.sql.catalog.lake_gold.warehouse": "gold",
    "spark.sql.catalog.lake_gold.scope": "lakekeeper",
    "spark.sql.catalog.lake_gold.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
    # Các cấu hình chung khác
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.jars.packages": ",".join(spark_jars_packages),
}

spark_conf = SparkConf().setAppName("Silver-to-Gold")
for k, v in config.items():
    spark_conf = spark_conf.set(k, v)

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# ===============================
# Read from silver
# ===============================
try:
    logger.info("🔎 Read data from lake_silver.silver.business_stats_clean ...")
    df = spark.sql("SELECT * FROM lake_silver.silver.business_stats_clean")
    logger.info("✅ Load silver successfully")
    df.printSchema()
    logger.info(f"📊 Total rows in silver: {df.count()}")
    df.limit(5).show()
    df.createOrReplaceTempView("silver_stats")
except Exception as e:
    logger.error(f"❌ Error reading silver: {e}", exc_info=True)
    raise

# ===============================
# Transform → gold
# ===============================
try:
    transformed = spark.sql(
        """
        SELECT *
        FROM silver_stats
        """
    )
    transformed.printSchema()
    transformed.limit(5).show()
    transformed.createOrReplaceTempView("gold_stats")
except Exception as e:
    logger.error(f"❌ Error transforming: {e}", exc_info=True)
    raise

# ===============================
# Write to gold
# ===============================
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake_gold.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lake_gold.gold.business_stats_summary
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        AS SELECT * FROM gold_stats
        """
    )
    logger.info("✅ Load data to gold successfully")
except Exception as e:
    logger.error(f"❌ Error writing to gold: {e}", exc_info=True)
    raise
