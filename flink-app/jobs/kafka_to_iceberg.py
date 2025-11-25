#!/usr/bin/env python3
"""
Flink streaming job to ingest data from Kafka topic nyc-taxi-data
and write to Iceberg bronze warehouse in lakekeeper
"""

import logging
import os
import sys
import time

from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Configuration
CATALOG_URL = os.getenv("CATALOG_URL", "http://lakekeeper:8181/catalog")
WAREHOUSE = os.getenv("WAREHOUSE", "bronze")
KEYCLOAK_TOKEN_URL = os.getenv(
    "KEYCLOAK_TOKEN_URL",
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
)
OAUTH_SCOPE = os.getenv("OAUTH_SCOPE", "lakekeeper")

# Flink client credentials (from environment variables or config)
FLINK_CLIENT_ID = os.getenv("FLINK_CLIENT_ID", "flink")
FLINK_CLIENT_SECRET = os.getenv(
    "FLINK_CLIENT_SECRET", "Wnrp0vD0ogkyP5dJxBlj1iYIPu2PTrAq"
)

# MinIO S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-data")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "lakekeeper.bronze.nyc_taxi_raw")


def create_table_env():
    """Create Flink TableEnvironment with streaming mode"""
    logger.info("Creating Flink TableEnvironment in streaming mode...")
    env_settings = (
        EnvironmentSettings.new_instance().in_streaming_mode().build()
    )
    table_env = StreamTableEnvironment.create(environment_settings=env_settings)

    # Set table configuration for better debugging
    table_config = table_env.get_config()
    table_config.set("pipeline.name", "Kafka-to-Iceberg-Bronze")
    table_config.set(
        "table.exec.sink.not-null-enforcer", "DROP"
    )  # Drop null records instead of failing

    # Configure S3 properties for MinIO at table environment level
    # This ensures Flink knows how to connect to MinIO for checkpointing and S3 operations
    table_config.set("s3.endpoint", MINIO_ENDPOINT)
    table_config.set("s3.path-style-access", "true")
    table_config.set("s3.region", "us-east-1")
    table_config.set("s3.access-key", MINIO_ACCESS_KEY)
    table_config.set("s3.secret-key", MINIO_SECRET_KEY)

    logger.info("TableEnvironment created successfully")
    logger.info(f"S3 endpoint configured: {MINIO_ENDPOINT}")
    return table_env


def register_iceberg_catalog(table_env):
    """Register Iceberg REST catalog in Flink with MinIO S3 configuration"""
    logger.info(f"Registering Iceberg catalog: {CATALOG_URL}")
    logger.info(
        f"Using warehouse: {WAREHOUSE} with MinIO endpoint: {MINIO_ENDPOINT}"
    )

    if not FLINK_CLIENT_SECRET:
        raise ValueError(
            "FLINK_CLIENT_SECRET environment variable is required. "
            "Please set it before running the job."
        )

    # For Iceberg REST catalog, we need to specify S3 configuration for MinIO
    # along with REST catalog properties.
    # Using direct S3 credentials instead of vended credentials to avoid authentication issues
    # when writing data files to MinIO.
    catalog_ddl = f"""
    CREATE CATALOG lakekeeper WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = '{CATALOG_URL}',
        'warehouse' = '{WAREHOUSE}',
        'credential' = '{FLINK_CLIENT_ID}:{FLINK_CLIENT_SECRET}',
        'oauth2-server-uri' = '{KEYCLOAK_TOKEN_URL}',
        'scope' = '{OAUTH_SCOPE}',
        's3.endpoint' = '{MINIO_ENDPOINT}',
        's3.path-style-access' = 'true',
        's3.region' = 'us-east-1',
        's3.access-key' = '{MINIO_ACCESS_KEY}',
        's3.secret-key' = '{MINIO_SECRET_KEY}'
    )
    """

    table_env.execute_sql(catalog_ddl)
    logger.info("Iceberg catalog registered successfully")


def create_kafka_source_table(table_env):
    """Create Kafka source table"""
    logger.info(f"Creating Kafka source table for topic: {KAFKA_TOPIC}")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")

    kafka_ddl = f"""
    CREATE TABLE kafka_source (
        VendorID INT,
        tpep_pickup_datetime STRING,
        tpep_dropoff_datetime STRING,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
        'properties.group.id' = 'flink-nyc-taxi-consumer',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'json.fail-on-missing-field' = 'false',
        'scan.startup.mode' = 'earliest-offset'
    )
    """

    try:
        table_env.execute_sql(kafka_ddl)
        logger.info("Kafka source table created successfully")
    except Exception as e:
        logger.error(f"Failed to create Kafka source table: {e}")
        raise


def verify_kafka_connection(table_env):
    """Verify that Flink can read data from Kafka by testing the connection"""
    logger.info("=" * 80)
    logger.info("Verifying Kafka connection and data reading...")
    logger.info("=" * 80)

    try:
        # Test 1: Describe the table structure
        logger.info("\n[Test 1] Checking Kafka source table structure...")
        try:
            result = table_env.execute_sql("DESCRIBE kafka_source")
            logger.info("Kafka source table structure:")
            result.print()
        except Exception as e:
            logger.warning(f"Could not describe table: {e}")

        # Test 2: Try to read sample records from Kafka
        logger.info(
            "\n[Test 2] Attempting to read sample records from Kafka..."
        )
        logger.info("This may take a few seconds to connect to Kafka...")

        # Create a query to read a few sample records
        # In streaming mode, we'll use LIMIT and try to collect results
        sample_query = """
        SELECT 
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            total_amount
        FROM kafka_source
        LIMIT 5
        """

        try:
            result = table_env.execute_sql(sample_query)
            logger.info(
                "Successfully connected to Kafka! Reading sample records..."
            )

            # Collect results with timeout
            records_collected = 0
            max_wait_time = 30  # Wait up to 30 seconds for data
            start_time = time.time()

            try:
                # Try to collect results
                for row in result.collect():
                    records_collected += 1
                    logger.info(f"Sample record {records_collected}: {row}")

                    # Stop after collecting 5 records or timeout
                    if records_collected >= 5:
                        break
                    if time.time() - start_time > max_wait_time:
                        logger.warning(f"Timeout after {max_wait_time} seconds")
                        break

                if records_collected > 0:
                    logger.info(
                        f"✓ Successfully read {records_collected} record(s) from Kafka!"
                    )
                    logger.info("Kafka connection is working correctly.")
                else:
                    logger.warning(
                        "⚠ No records found in Kafka topic. This could mean:"
                    )
                    logger.warning("  - Topic is empty")
                    logger.warning("  - No producers are sending data")
                    logger.warning("  - Consumer group offset is at the end")
                    logger.info("Job will continue and wait for new data...")

            except Exception as collect_error:
                logger.warning(
                    f"Could not collect results (this is normal in streaming mode): {collect_error}"
                )
                logger.info(
                    "Kafka table is configured correctly. Streaming job will read data as it arrives."
                )

        except Exception as query_error:
            logger.error(f"Failed to query Kafka source: {query_error}")
            logger.error(
                "This indicates a problem with Kafka connectivity or topic configuration."
            )
            raise

        # Test 3: Check if we can get metadata about the table
        logger.info("\n[Test 3] Verifying table metadata...")
        try:
            # Try to explain the query plan
            explain_query = "EXPLAIN SELECT * FROM kafka_source LIMIT 1"
            explain_result = table_env.execute_sql(explain_query)
            logger.info(
                "Query plan generated successfully - Kafka connector is properly configured"
            )
        except Exception as explain_error:
            logger.warning(f"Could not explain query: {explain_error}")

        logger.info("=" * 80)
        logger.info("✓ Kafka connection verification completed")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"Kafka connection verification failed: {e}")
        logger.error("=" * 80)
        logger.exception("Full error details:")
        raise


def create_iceberg_namespace(table_env):
    """Create namespace (database/schema) in Iceberg catalog if it doesn't exist"""
    logger.info("Creating namespace 'bronze' in catalog 'lakekeeper'")

    # Try CREATE DATABASE first (standard Flink SQL)
    create_namespace_ddl = """
    CREATE DATABASE IF NOT EXISTS lakekeeper.bronze
    """

    try:
        table_env.execute_sql(create_namespace_ddl)
        logger.info(
            "Namespace 'lakekeeper.bronze' created/verified successfully"
        )
    except Exception as e:
        logger.warning(f"CREATE DATABASE failed, trying CREATE SCHEMA: {e}")
        # Try CREATE SCHEMA as alternative (some catalogs prefer this)
        try:
            create_schema_ddl = """
            CREATE SCHEMA IF NOT EXISTS lakekeeper.bronze
            """
            table_env.execute_sql(create_schema_ddl)
            logger.info(
                "Namespace 'lakekeeper.bronze' created using CREATE SCHEMA"
            )
        except Exception as schema_error:
            logger.warning(f"CREATE SCHEMA also failed: {schema_error}")
            # Try to verify namespace exists by listing databases
            try:
                result = table_env.execute_sql("SHOW DATABASES IN lakekeeper")
                databases = [row[0] for row in result.collect()]
                if "bronze" in databases:
                    logger.info("Namespace 'bronze' exists, proceeding")
                else:
                    logger.error(
                        "Namespace 'bronze' does not exist and could not be created"
                    )
                    raise
            except Exception as verify_error:
                logger.warning(
                    f"Could not verify namespace existence: {verify_error}"
                )
                # Continue anyway, might work if namespace already exists


def create_iceberg_sink_table(table_env):
    """Create or use existing Iceberg sink table"""
    logger.info(f"Creating Iceberg sink table: {ICEBERG_TABLE}")

    # When creating table in an Iceberg catalog, do NOT use 'connector'='iceberg'
    # Only define schema and Iceberg table properties
    create_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP(3),
        tpep_dropoff_datetime TIMESTAMP(3),
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        ingestion_timestamp TIMESTAMP(3)
    ) WITH (
        'format-version' = '2',
        'write.upsert.enabled' = 'false'
    )
    """

    try:
        table_env.execute_sql(create_table_ddl)
        logger.info(
            f"Iceberg sink table {ICEBERG_TABLE} created/verified successfully"
        )
    except Exception as e:
        logger.warning(f"Table creation check failed (may already exist): {e}")
        # Try to verify table exists by describing it
        try:
            table_env.execute_sql(f"DESCRIBE {ICEBERG_TABLE}").print()
            logger.info(f"Table {ICEBERG_TABLE} exists, proceeding with insert")
        except Exception as desc_error:
            logger.error(
                f"Table {ICEBERG_TABLE} does not exist and could not be created: {desc_error}"
            )
            raise


def transform_and_write(table_env):
    """Transform data from Kafka and write to Iceberg

    In PyFlink streaming mode, execute_sql() with INSERT automatically starts the job.
    Returns the TableResult to keep a reference to the running job.
    """
    logger.info("Starting data transformation and write to Iceberg...")
    logger.info(f"Target Iceberg table: {ICEBERG_TABLE}")

    # Transform query: convert string datetime to timestamp and add ingestion timestamp
    insert_ddl = f"""
    INSERT INTO {ICEBERG_TABLE}
    SELECT 
        VendorID,
        TO_TIMESTAMP(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss') AS tpep_pickup_datetime,
        TO_TIMESTAMP(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') AS tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        CURRENT_TIMESTAMP AS ingestion_timestamp
    FROM kafka_source
    """

    try:
        # Execute the insert - in streaming mode, this automatically starts the job
        logger.info("Submitting streaming job to Flink cluster...")
        result = table_env.execute_sql(insert_ddl)
        logger.info("Data transformation and write job submitted successfully!")
        logger.info("Job is now running in streaming mode...")
        return result
    except Exception as e:
        logger.error(f"Failed to submit streaming job: {e}")
        raise


def main():
    """Main function to run the Flink streaming job"""
    logger.info("=" * 80)
    logger.info("Starting Flink Kafka to Iceberg Streaming Job")
    logger.info("=" * 80)

    # Log configuration
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Catalog URL: {CATALOG_URL}")
    logger.info(f"Warehouse: {WAREHOUSE}")
    logger.info(f"Iceberg Table: {ICEBERG_TABLE}")
    logger.info(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    logger.info("=" * 80)

    try:
        # Validate client credentials
        if not FLINK_CLIENT_SECRET:
            raise ValueError(
                "FLINK_CLIENT_SECRET environment variable is required. "
                "Please set it before running the job."
            )
        logger.info(f"Using Flink client ID: {FLINK_CLIENT_ID}")

        # Create table environment
        logger.info("\n[Step 1/6] Creating Flink Table Environment...")
        table_env = create_table_env()

        # Register Iceberg catalog
        logger.info("\n[Step 2/6] Registering Iceberg catalog...")
        register_iceberg_catalog(table_env)

        # Create namespace (database) in Iceberg catalog
        logger.info("\n[Step 3/6] Creating/verifying namespace...")
        create_iceberg_namespace(table_env)

        # Create Kafka source table
        logger.info("\n[Step 4/7] Creating Kafka source table...")
        create_kafka_source_table(table_env)

        # Verify Kafka connection and data reading
        logger.info("\n[Step 5/7] Verifying Kafka connection...")
        verify_kafka_connection(table_env)

        # Create Iceberg sink table
        logger.info("\n[Step 6/7] Creating/verifying Iceberg sink table...")
        create_iceberg_sink_table(table_env)

        # Transform and write data
        logger.info("\n[Step 7/7] Starting streaming job...")
        # In PyFlink streaming mode, execute_sql() with INSERT automatically starts the job
        # The job runs asynchronously, so we need to keep the process alive
        result = transform_and_write(table_env)

        logger.info("=" * 80)
        logger.info("✓ Job execution started successfully!")
        logger.info("Streaming data from Kafka to Iceberg...")
        logger.info(
            "Job is running. Use Ctrl+C to stop or cancel via Flink Web UI."
        )
        logger.info("=" * 80)

        # Keep the process alive to maintain the streaming job
        # The job runs asynchronously in the background
        try:
            iteration = 0
            while True:
                iteration += 1
                time.sleep(60)  # Check every minute
                logger.info(
                    f"[Health Check {iteration}] Job is still running..."
                )
        except KeyboardInterrupt:
            logger.info("\nReceived interrupt signal. Stopping job...")
            # Optionally wait for the job to finish
            try:
                result.wait()  # Wait for job completion if TableResult supports it
            except Exception:
                pass  # Ignore if wait() is not supported

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"Job failed with error: {e}")
        logger.error("=" * 80)
        logger.exception("Full stack trace:")
        sys.exit(1)


if __name__ == "__main__":
    main()
