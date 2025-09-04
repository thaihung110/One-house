import logging

import pandas as pd
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


CATALOG_URL = "http://lakekeeper:8181/catalog"
WAREHOUSE = "bronze"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = ".".join(SPARK_VERSION.split(".")[:2])
ICEBERG_VERSION = "1.6.1"


# Thay print bằng log
logger.info(f"SPARK_VERSION: {SPARK_VERSION}")
logger.info(f"SPARK_MINOR_VERSION: {SPARK_MINOR_VERSION}")
logger.info(f"ICEBERG_VERSION: {ICEBERG_VERSION}")


spark_jars_packages = (
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
)

config = {
    # Catalog configuration
    "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakekeeper.type": "rest",
    "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
    "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
    "spark.sql.catalog.lakekeeper.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Iceberg extensions
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "lakekeeper",
    # Jars packages
    "spark.jars.packages": ",".join(spark_jars_packages),
}

spark_config = SparkConf().setMaster("local").setAppName("Iceberg-REST")
for k, v in config.items():
    spark_config = spark_config.set(k, v)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

logger.info("Read data from source")
source_path = "/opt/spark/data/business_data.csv"

df = spark.read.option("header", "true").csv(source_path)
logger.info(f"Read {df.count()} rows from {source_path}")
df.printSchema()

table_name = "lakekeeper.bronze.business_data"

# Đăng ký dataframe source thành temp view
df.createOrReplaceTempView("source_stats")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakekeeper.test")
logger.info("Create namespace lakekeeper.test successfully")

# Nếu muốn tạo bảng mới từ source data
spark.sql(
    """
    CREATE OR REPLACE TABLE lakekeeper.test.business_stats
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
    AS SELECT * FROM source_stats
    """
)

# Nếu muốn append thêm dữ liệu vào bảng đã tồn tại
spark.sql(
    """
    INSERT INTO lakekeeper.test.business_stats
    SELECT * FROM source_stats
"""
)

logger.info("Load data to bronze table successfully")
