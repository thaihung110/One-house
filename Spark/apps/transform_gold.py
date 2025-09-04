import logging

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CATALOG_URL = "http://lakekeeper:8181/catalog"
ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])

logger.info(f"SPARK_VERSION: {SPARK_VERSION}")
logger.info(f"SPARK_MINOR_VERSION: {SPARK_MINOR_VERSION}")
logger.info(f"ICEBERG_VERSION: {ICEBERG_VERSION}")

spark_jars_packages = f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION}"

config = {
    # Silver catalog
    "spark.sql.catalog.lake_silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_silver.type": "rest",
    "spark.sql.catalog.lake_silver.uri": CATALOG_URL,
    "spark.sql.catalog.lake_silver.warehouse": "silver",
    "spark.sql.catalog.lake_silver.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Gold catalog
    "spark.sql.catalog.lake_gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_gold.type": "rest",
    "spark.sql.catalog.lake_gold.uri": CATALOG_URL,
    "spark.sql.catalog.lake_gold.warehouse": "gold",
    "spark.sql.catalog.lake_gold.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Iceberg extensions
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.jars.packages": spark_jars_packages,
    "spark.sql.execution.arrow.pyspark.enabled": "false",
}

spark_conf = SparkConf().setAppName("Silver-to-Gold")
for k, v in config.items():
    spark_conf = spark_conf.set(k, v)

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# ===============================
# Đọc từ silver
# ===============================
try:
    logger.info("🔎 Đọc dữ liệu từ lake_silver.test.business_stats_clean ...")
    df = spark.sql("SELECT * FROM lake_silver.test.business_stats_clean")
    logger.info("✅ Load dữ liệu silver thành công")
    df.printSchema()
    logger.info(f"📊 Tổng số bản ghi silver: {df.count()}")
    df.limit(5).show()
    df.createOrReplaceTempView("silver_stats")
except Exception as e:
    logger.error(f"❌ Lỗi khi đọc silver: {e}", exc_info=True)
    raise

# ===============================
# Transform → gold
# ===============================
try:
    transformed = spark.sql(
        """
        SELECT
            period_date,
            Series_title_2 AS industry,
            SUM(Data_value) AS total_sales,
            COUNT(*) AS record_count
        FROM silver_stats
        GROUP BY period_date, Series_title_2
        ORDER BY period_date, industry
        """
    )
    logger.info("✅ Transform silver → gold thành công")
    transformed.printSchema()
    transformed.limit(5).show()
    transformed.createOrReplaceTempView("gold_stats")
except Exception as e:
    logger.error(f"❌ Lỗi khi transform: {e}", exc_info=True)
    raise

# ===============================
# Ghi vào gold
# ===============================
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake_gold.test")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lake_gold.test.business_stats_summary
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        AS SELECT * FROM gold_stats
        """
    )
    logger.info("🎉 Load dữ liệu vào gold thành công ✅")
except Exception as e:
    logger.error(f"❌ Lỗi khi ghi gold: {e}", exc_info=True)
    raise
