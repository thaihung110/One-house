#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka brokers to be ready..."
sleep 10

# Kafka broker addresses (internal listener)
KAFKA_BROKERS="kafka1:9092,kafka2:9092"

# Create csv-upload-events topic
echo "Creating csv-upload-events topic..."
kafka-topics --create \
  --bootstrap-server $KAFKA_BROKERS \
  --topic csv-upload-events \
  --partitions 1 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config cleanup.policy=delete \
  --if-not-exists

# Verify topic creation
echo "Verifying topic creation..."
kafka-topics --describe \
  --bootstrap-server $KAFKA_BROKERS \
  --topic csv-upload-events

echo "Topic csv-upload-events created successfully!"

# Create nyc-taxi-data topic
echo "Creating nyc-taxi-data topic..."
kafka-topics --create \
  --bootstrap-server $KAFKA_BROKERS \
  --topic nyc-taxi-data \
  --partitions 2 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config cleanup.policy=delete \
  --if-not-exists

# Verify nyc-taxi-data topic creation
echo "Verifying nyc-taxi-data topic creation..."
kafka-topics --describe \
  --bootstrap-server $KAFKA_BROKERS \
  --topic nyc-taxi-data

echo "Topic nyc-taxi-data created successfully!"

