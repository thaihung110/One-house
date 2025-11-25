# Airflow Spark Integration Guide

## Overview
Airflow được cấu hình để submit Spark jobs đến Spark cluster thông qua `SparkSubmitOperator`. Tài liệu này mô tả các cấu hình quan trọng.

## Cấu hình chính

### 1. Docker Image Customization
**File:** `airflow/Dockerfile`

- Base image: `apache/airflow:2.9.1`
- Cài đặt: `openjdk-17-jdk-headless`, `procps` (cho `spark-submit`)
- Set `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`

**Lý do:** Airflow container cần Java và `ps` command để chạy `spark-submit`.

### 2. Dependencies trong Airflow Container
**File:** `docker-compose-airflow.yaml`

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-apache-kafka==1.4.1 apache-airflow-providers-apache-spark==4.0.1 requests confluent-kafka==2.6.0 pyspark==3.5.2"
JAVA_HOME: "/usr/lib/jvm/java-17-openjdk-amd64"
AIRFLOW_CONN_SPARK_DEFAULT: "spark://spark-master:7077"
```

**Quan trọng:**
- `pyspark==3.5.2`: Phiên bản phải khớp với Spark cluster (3.5.7)
- `apache-airflow-providers-apache-spark==4.0.1`: Provider cho SparkSubmitOperator
- `JAVA_HOME`: Phải khớp với JDK version trong Dockerfile

### 3. Spark Job Packages (trong DAG)
**File:** `airflow/dags/nyc_taxi_silver.py`

```python
packages=(
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
```

**Tại sao phải set ở DAG level:**
- Khi submit job từ Airflow đến Spark cluster, Spark cluster cần tải packages từ Maven **trước khi** job code chạy
- `spark.jars.packages` trong Python code chỉ hoạt động khi Spark session đã được tạo (quá muộn)
- Command-line `--packages` được xử lý trước, đảm bảo packages có sẵn khi job khởi động

### 4. S3A/MinIO Configuration
**File:** `airflow/dags/nyc_taxi_silver.py`

```python
conf={
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}
```

**Quan trọng:**
- `fs.s3a.path.style.access=true`: Bắt buộc cho MinIO (không phải AWS S3)
- Endpoint: `http://minio:9000` (internal Docker network)
- Credentials: Lấy từ `docker-compose.yaml` (MINIO_ROOT_USER/PASSWORD)

### 5. Iceberg Configuration
**File:** `airflow/dags/nyc_taxi_silver.py`

```python
conf={
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Catalog config được set trong job code (load_silver.py)
}
```

**Lưu ý:**
- Iceberg extensions phải được enable ở DAG level
- Catalog URI, credentials, warehouse được config trong job code vì cần runtime values

## Volume Mounts

```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./airflow/logs:/opt/airflow/logs
  - ./airflow/plugins:/opt/airflow/plugins
  - ./spark/apps:/opt/airflow/spark-apps  # Spark job scripts
```

**Quan trọng:** Spark job scripts phải được mount vào Airflow container để `SparkSubmitOperator` có thể truy cập.

## Troubleshooting

### Lỗi: "JAVA_HOME is not set"
- Kiểm tra `JAVA_HOME` trong `docker-compose-airflow.yaml` khớp với Dockerfile
- Restart Airflow containers sau khi thay đổi

### Lỗi: "ClassNotFoundException: S3AFileSystem"
- Đảm bảo `hadoop-aws` và `aws-java-sdk-bundle` có trong `packages` của DAG
- Kiểm tra S3A config trong `conf` dict

### Lỗi: "Cannot find catalog plugin class"
- Đảm bảo Iceberg packages (`iceberg-spark-runtime`, `iceberg-aws-bundle`) có trong `packages`
- Kiểm tra `spark.sql.extensions` được set trong `conf`

### Packages không được tải
- Luôn set `packages` ở DAG level (SparkSubmitOperator), không chỉ trong job code
- Kiểm tra network connectivity từ Spark cluster đến Maven repositories

## Best Practices

1. **Packages:** Luôn declare ở DAG level để đảm bảo Spark cluster tải trước khi job chạy
2. **Config:** S3A/MinIO config ở DAG level, Iceberg catalog config ở job code
3. **Version matching:** Đảm bảo PySpark version trong Airflow container khớp với Spark cluster
4. **Java version:** Sử dụng Java 17 (OpenJDK 17) cho compatibility với Spark 3.5.x

