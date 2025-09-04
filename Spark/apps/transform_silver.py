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
    # Bronze catalog
    "spark.sql.catalog.lake_bronze": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_bronze.type": "rest",
    "spark.sql.catalog.lake_bronze.uri": CATALOG_URL,
    "spark.sql.catalog.lake_bronze.warehouse": "bronze",
    "spark.sql.catalog.lake_bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Silver catalog
    "spark.sql.catalog.lake_silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake_silver.type": "rest",
    "spark.sql.catalog.lake_silver.uri": CATALOG_URL,
    "spark.sql.catalog.lake_silver.warehouse": "silver",
    "spark.sql.catalog.lake_silver.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Iceberg extensions
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.jars.packages": spark_jars_packages,
    "spark.sql.execution.arrow.pyspark.enabled": "false",
}

spark_conf = SparkConf().setAppName("Bronze-to-Silver")
for k, v in config.items():
    spark_conf = spark_conf.set(k, v)

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# ===============================
# Đọc từ bronze
# ===============================
try:
    logger.info("🔎 Đọc dữ liệu từ lake_bronze.test.business_stats ...")
    df = spark.sql("SELECT * FROM lake_bronze.test.business_stats")
    logger.info(f"Schema:")
    df.printSchema()
    count = df.count()
    logger.info(f"Đọc {count} bản ghi từ bronze")

    df.createOrReplaceTempView("bronze_stats")
except Exception as e:
    logger.error(f"❌ Lỗi khi đọc bronze: {e}")
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
    logger.info("✅ Transform query compiled xong")

    # Debug: chỉ show sample để tránh crash executor
    logger.info("🔎 Schema sau transform:")
    transformed.printSchema()

    logger.info("🔎 Sample 5 records:")
    transformed.limit(5).show()

    # Debug: log query plan
    logger.info("🔎 Query plan:")
    transformed.explain(True)

    transformed.createOrReplaceTempView("silver_stats")
except Exception as e:
    logger.error(f"❌ Lỗi khi transform: {e}", exc_info=True)
    raise

# ===============================
# Ghi vào silver
# ===============================
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake_silver.test")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lake_silver.test.business_stats_clean
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        AS SELECT * FROM silver_stats
        """
    )
    logger.info("✅ Transform từ bronze sang silver thành công")
except Exception as e:
    logger.error(f"❌ Lỗi khi ghi vào silver: {e}")
    raise
