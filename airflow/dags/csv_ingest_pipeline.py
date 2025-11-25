"""
CSV Ingest Pipeline DAG

This DAG triggers NiFi flow to ingest data from datasource-api to MinIO/Iceberg.

Architecture:
    Airflow Schedule/Manual -> Check API -> Trigger NiFi -> NiFi Ingests to MinIO
"""

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

from airflow import DAG

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
DATASOURCE_API_BASE_URL = "http://datasource-api:8000"
NIFI_API_BASE_URL = "http://nifi:8080/nifi-api"
NIFI_ROOT_PROCESS_GROUP_ID = "root"  # Default root process group
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv-upload-events")
KAFKA_CONSUMER_GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP_ID", "airflow-csv-listener"
)
KAFKA_POLL_TIMEOUT = int(os.getenv("KAFKA_POLL_TIMEOUT", "10"))

# Default table to ingest (can be overridden via DAG params)
DEFAULT_TABLE_NAME = "nyc_taxi"

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


class KafkaCsvUploadSensor(BaseSensorOperator):
    """
    Sensor that waits for a *new* csv_uploaded event on Kafka.

    The consumer is kept alive across poke cycles and starts from the end of the
    topic so it only reacts to messages that arrive after the sensor begins.
    """

    template_fields = ("topic",)

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        poll_timeout: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.poll_timeout = poll_timeout
        self._consumer = None
        self._seeked_to_end = False

    def _ensure_consumer(self):
        from confluent_kafka import OFFSET_END, Consumer

        if self._consumer is not None:
            return

        consumer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "max.poll.interval.ms": 600000,
        }

        def _on_assign(consumer, partitions):
            if not self._seeked_to_end:
                for partition in partitions:
                    partition.offset = OFFSET_END
                self._seeked_to_end = True
            consumer.assign(partitions)

        self._consumer = Consumer(consumer_conf)
        self._consumer.subscribe([self.topic], on_assign=_on_assign)

        self.log.info(
            "Kafka consumer ready on topic '%s' waiting for new messages",
            self.topic,
        )

    def poke(self, context):
        from confluent_kafka import KafkaException

        self._ensure_consumer()
        message = self._consumer.poll(timeout=self.poll_timeout)

        if message is None:
            self.log.debug("No Kafka message yet")
            return False

        if message.error():
            self.log.warning("Kafka error: %s", message.error())
            return False

        try:
            payload = json.loads(message.value().decode("utf-8"))
        except Exception as exc:
            self.log.warning("Failed to decode Kafka payload: %s", exc)
            self._consumer.commit(message=message)
            return False

        if payload.get("event_type") != "csv_uploaded":
            self.log.info(
                "Skipping Kafka event type %s", payload.get("event_type")
            )
            self._consumer.commit(message=message)
            return False

        table_name = payload.get("table_name")
        self.log.info(
            "Received csv_uploaded event for table %s (rows: %s)",
            table_name,
            payload.get("rows_count"),
        )
        context["ti"].xcom_push(key="kafka_message", value=payload)
        self._consumer.commit(message=message)
        return True

    def on_kill(self):
        if self._consumer:
            self._consumer.close()


def check_datasource_api(**context):
    """
    Check if datasource API is available and get table info

    Returns:
        dict: Table information including record count
    """
    try:
        ti = context["ti"]
        kafka_message = ti.xcom_pull(
            task_ids="wait_for_csv_upload_event", key="kafka_message"
        )

        # Get table name from Kafka message, DAG params, or default
        if kafka_message and kafka_message.get("table_name"):
            table_name = kafka_message["table_name"]
        else:
            table_name = context.get("params", {}).get(
                "table_name", DEFAULT_TABLE_NAME
            )

        logger.info(f"Checking datasource API for table: {table_name}")

        # Check API health
        health_response = requests.get(
            f"{DATASOURCE_API_BASE_URL}/health", timeout=10
        )

        if health_response.status_code != 200:
            raise Exception(
                f"Datasource API health check failed: {health_response.status_code}"
            )

        logger.info("Datasource API is healthy")

        # Get table info
        tables_response = requests.get(
            f"{DATASOURCE_API_BASE_URL}/api/tables", timeout=30
        )

        if tables_response.status_code != 200:
            raise Exception(
                f"Failed to get tables: {tables_response.status_code}"
            )

        tables_data = tables_response.json()

        # Find our table
        table_info = None
        for table in tables_data.get("tables", []):
            if table.get("table_name") == table_name:
                table_info = table
                break

        if not table_info:
            raise Exception(f"Table '{table_name}' not found in datasource API")

        logger.info(
            f"Found table: {table_name}, rows: {table_info.get('row_count')}"
        )

        # Push to XCom for next task
        result = {
            "table_name": table_name,
            "row_count": table_info.get("row_count"),
            "columns": table_info.get("columns", []),
            "api_endpoint": f"/api/data/{table_name}",
        }
        if kafka_message:
            result["kafka_message"] = kafka_message
        return result

    except Exception as e:
        logger.error(f"Error checking datasource API: {e}", exc_info=True)
        raise


def trigger_nifi_flow(**context):
    """
    Trigger NiFi "API to Minio" Process Group to start the ingestion flow

    This will start all processors in the Process Group to run the full flow
    """
    try:
        # Get table info from previous task
        ti = context["ti"]
        table_info = ti.xcom_pull(task_ids="check_datasource_api")

        if not table_info:
            raise Exception("No table info from previous task")

        table_name = table_info["table_name"]
        row_count = table_info["row_count"]

        logger.info(
            f"Triggering NiFi flow for table: {table_name}, rows: {row_count}"
        )

        # Step 1: Find the "API to Minio" process group
        logger.info("Finding 'API to Minio' process group in NiFi...")

        # Get root process group flow
        flow_response = requests.get(
            f"{NIFI_API_BASE_URL}/flow/process-groups/{NIFI_ROOT_PROCESS_GROUP_ID}",
            timeout=30,
        )

        if flow_response.status_code != 200:
            raise Exception(
                f"Failed to get NiFi flow: {flow_response.status_code}"
            )

        flow_data = flow_response.json()

        # Find "API to Minio" process group
        target_pg = None
        process_groups = (
            flow_data.get("processGroupFlow", {})
            .get("flow", {})
            .get("processGroups", [])
        )

        for pg in process_groups:
            component = pg.get("component", {})
            pg_name = component.get("name", "").lower()
            # Match both "API to Minio" and "api to minio"
            if "api" in pg_name and "minio" in pg_name:
                target_pg = pg
                break

        if not target_pg:
            raise Exception(
                "Process Group 'API to Minio' not found. Please create it in NiFi UI."
            )

        process_group_id = target_pg["id"]
        pg_name = target_pg.get("component", {}).get("name")
        logger.info(f"Found process group: {pg_name} (ID: {process_group_id})")

        # Step 2: Get current process group state
        pg_detail = requests.get(
            f"{NIFI_API_BASE_URL}/process-groups/{process_group_id}", timeout=30
        )

        if pg_detail.status_code != 200:
            raise Exception(
                f"Failed to get process group details: {pg_detail.status_code}"
            )

        pg_data = pg_detail.json()
        revision = pg_data["revision"]

        logger.info(
            f"Current process group state: {pg_data['component']['runningCount']} processors running"
        )

        # Step 3: Start all processors in the process group
        logger.info("Starting all processors in process group...")

        start_payload = {
            "id": process_group_id,
            "state": "RUNNING",
        }

        start_response = requests.put(
            f"{NIFI_API_BASE_URL}/flow/process-groups/{process_group_id}",
            json=start_payload,
            timeout=30,
        )

        if start_response.status_code not in [200, 201]:
            raise Exception(
                f"Failed to start process group: {start_response.status_code} - {start_response.text}"
            )

        logger.info("✅ Process group started successfully")

        # Step 4: Wait for processing to complete (optional)
        import time

        logger.info("Waiting for flow to process data...")
        time.sleep(5)  # Give it time to start and process

        # Check if queues are empty (processing complete)
        for i in range(12):  # Check for up to 60 seconds (12 * 5)
            pg_status = requests.get(
                f"{NIFI_API_BASE_URL}/flow/process-groups/{process_group_id}",
                timeout=30,
            )

            if pg_status.status_code == 200:
                status_data = pg_status.json()
                queued = (
                    status_data.get("processGroupFlow", {})
                    .get("breadcrumb", {})
                    .get("breadcrumb", {})
                    .get("queued", "0")
                )

                logger.info(f"Check {i+1}/12 - Queued: {queued}")

                # If queued is "0 / 0 bytes", processing is complete
                if queued.startswith("0"):
                    logger.info("Processing complete - all queues empty")
                    break

            time.sleep(5)

        # Step 5: Stop all processors (cleanup)
        logger.info("Stopping process group...")

        stop_payload = {
            "id": process_group_id,
            "state": "STOPPED",
        }

        stop_response = requests.put(
            f"{NIFI_API_BASE_URL}/flow/process-groups/{process_group_id}",
            json=stop_payload,
            timeout=30,
        )

        if stop_response.status_code in [200, 201]:
            logger.info("Process group stopped")
        else:
            logger.warning(
                f"Failed to stop process group: {stop_response.status_code}"
            )

        return {
            "status": "success",
            "table_name": table_name,
            "row_count": row_count,
            "process_group_id": process_group_id,
            "process_group_name": pg_name,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error triggering NiFi flow: {e}", exc_info=True)
        raise


# Define the DAG
with DAG(
    dag_id="csv_ingest_pipeline",
    default_args=default_args,
    description="CSV ingestion pipeline: Datasource API -> NiFi -> MinIO",
    schedule_interval=None,  # Manual trigger or external trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nifi", "minio", "datasource-api"],
    params={
        "table_name": DEFAULT_TABLE_NAME,  # Can be overridden when triggering
    },
) as dag:

    wait_for_event = KafkaCsvUploadSensor(
        task_id="wait_for_csv_upload_event",
        topic=KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP_ID,
        poll_timeout=KAFKA_POLL_TIMEOUT,
        poke_interval=30,
        timeout=60 * 60 * 6,
    )

    # Task 1: Start marker
    start = EmptyOperator(
        task_id="start",
    )

    # Task 2: Check datasource API and get table info
    check_api = PythonOperator(
        task_id="check_datasource_api",
        python_callable=check_datasource_api,
        provide_context=True,
    )

    # Task 3: Trigger NiFi flow
    trigger_nifi = PythonOperator(
        task_id="trigger_nifi_flow",
        python_callable=trigger_nifi_flow,
        provide_context=True,
    )

    # Task 4: End marker
    end = EmptyOperator(
        task_id="end",
    )

    # Define task dependencies
    start >> wait_for_event >> check_api >> trigger_nifi >> end
