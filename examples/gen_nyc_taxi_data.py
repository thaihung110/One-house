#!/usr/bin/env python3
"""
Script to generate NYC taxi data continuously for 10 seconds
- Uploads data to datasource-api via /api/upload endpoint
- Sends data directly to Kafka topic nyc-taxi-data
"""

import io
import json
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from kafka import KafkaProducer

# Configuration
DATASOURCE_API_URL = "http://localhost:8083/api/upload"
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092,localhost:19094"
KAFKA_TOPIC = "nyc-taxi-data"
DURATION_SECONDS = 10
BATCH_SIZE = 5  # Number of records per batch

# NYC Taxi data schema
COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

# Valid ranges for data generation
VENDOR_IDS = [1, 2]
PASSENGER_COUNT_RANGE = (0, 6)
TRIP_DISTANCE_RANGE = (0.1, 25.0)
RATECODE_IDS = [1, 2, 3, 4, 5, 6]
STORE_AND_FWD_FLAGS = ["N", "Y"]
LOCATION_IDS = list(range(1, 265))  # NYC taxi zones
PAYMENT_TYPES = [1, 2, 3, 4, 5, 6]
EXTRA_VALUES = [0, 0.5, 3]
MTA_TAX_VALUES = [-0.5, 0, 0.5]
TIP_AMOUNT_RANGE = (0, 20)
TOLLS_AMOUNT_RANGE = (0, 10)
IMPROVEMENT_SURCHARGE_VALUES = [-0.3, 0, 0.3]
CONGESTION_SURCHARGE_VALUES = [-2.5, 0, 2.5]


def generate_taxi_record(base_time=None):
    """Generate a single NYC taxi record with random values"""
    if base_time is None:
        base_time = datetime.now()

    # Generate pickup time (within last hour)
    pickup_offset = random.randint(0, 3600)  # seconds
    pickup_time = base_time - timedelta(seconds=pickup_offset)

    # Generate dropoff time (5-60 minutes after pickup)
    trip_duration = random.randint(300, 3600)  # 5-60 minutes
    dropoff_time = pickup_time + timedelta(seconds=trip_duration)

    # Generate trip distance
    trip_distance = round(random.uniform(*TRIP_DISTANCE_RANGE), 2)

    # Generate fare amount based on distance
    base_fare = max(2.5, trip_distance * 2.5)
    fare_amount = round(base_fare + random.uniform(-2, 5), 2)

    # Generate other amounts
    extra = random.choice(EXTRA_VALUES)
    mta_tax = random.choice(MTA_TAX_VALUES)
    tip_amount = (
        round(random.uniform(*TIP_AMOUNT_RANGE), 2)
        if random.random() > 0.3
        else 0
    )
    tolls_amount = (
        round(random.uniform(*TOLLS_AMOUNT_RANGE), 2)
        if random.random() > 0.7
        else 0
    )
    improvement_surcharge = random.choice(IMPROVEMENT_SURCHARGE_VALUES)
    congestion_surcharge = random.choice(CONGESTION_SURCHARGE_VALUES)

    # Calculate total amount
    total_amount = round(
        fare_amount
        + extra
        + mta_tax
        + tip_amount
        + tolls_amount
        + improvement_surcharge
        + congestion_surcharge,
        2,
    )

    record = {
        "VendorID": random.choice(VENDOR_IDS),
        "tpep_pickup_datetime": pickup_time.strftime("%Y-%m-%d %H:%M:%S"),
        "tpep_dropoff_datetime": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": random.randint(*PASSENGER_COUNT_RANGE),
        "trip_distance": trip_distance,
        "RatecodeID": random.choice(RATECODE_IDS),
        "store_and_fwd_flag": random.choice(STORE_AND_FWD_FLAGS),
        "PULocationID": random.choice(LOCATION_IDS),
        "DOLocationID": random.choice(LOCATION_IDS),
        "payment_type": random.choice(PAYMENT_TYPES),
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "congestion_surcharge": congestion_surcharge,
    }
    return record


def upload_to_datasource_api(df, table_name="nyc_taxi_data"):
    """Upload DataFrame to datasource-api via /api/upload endpoint"""
    try:
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()

        # Prepare multipart form data
        files = {
            "file": (
                "nyc_taxi_data.csv",
                io.BytesIO(csv_content.encode("utf-8")),
                "text/csv",
            )
        }
        data = {"table_name": table_name}

        # Upload to API
        response = requests.post(
            DATASOURCE_API_URL,
            files=files,
            data=data,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error uploading to datasource-api: {e}")
        return None


def send_to_kafka(producer, records):
    """Send records to Kafka topic"""
    try:
        for record in records:
            message = json.dumps(record).encode("utf-8")
            future = producer.send(KAFKA_TOPIC, value=message)
            # Don't wait for confirmation to speed up
        producer.flush()
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return False


def main():
    print(f"Starting data generation for {DURATION_SECONDS} seconds...")
    print(f"Datasource API: {DATASOURCE_API_URL}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")

    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: v,
        )
        print("Kafka producer initialized successfully")
    except Exception as e:
        print(f"Warning: Failed to initialize Kafka producer: {e}")
        producer = None

    start_time = time.time()
    batch_count = 0
    total_records = 0

    try:
        while time.time() - start_time < DURATION_SECONDS:
            # Generate batch of records
            records = [generate_taxi_record() for _ in range(BATCH_SIZE)]
            df = pd.DataFrame(records)

            # Upload to datasource-api
            print(
                f"\nBatch {batch_count + 1}: Uploading {len(records)} records to datasource-api..."
            )
            api_result = upload_to_datasource_api(df)
            if api_result:
                print(
                    f"  ✓ Uploaded to datasource-api: {api_result.get('rows_inserted', 0)} rows"
                )

            # Send to Kafka
            if producer:
                print(
                    f"  Sending {len(records)} records to Kafka topic '{KAFKA_TOPIC}'..."
                )
                kafka_success = send_to_kafka(producer, records)
                if kafka_success:
                    print(f"  ✓ Sent to Kafka successfully")

            total_records += len(records)
            batch_count += 1

            # Small delay between batches
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nInterrupted by user")

    finally:
        if producer:
            producer.close()
            print("\nKafka producer closed")

    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Data generation completed!")
    print(f"  Duration: {elapsed_time:.2f} seconds")
    print(f"  Batches: {batch_count}")
    print(f"  Total records: {total_records}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
