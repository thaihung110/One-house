# Data Source API

API để upload file CSV (NYC Taxi data) và cung cấp REST endpoints cho NiFi để kéo dữ liệu.

## 🚀 Quick Start

```bash
# Khởi động services
docker-compose -f docker-compose-datasource.yaml up -d --build

# Upload NYC Taxi CSV
curl -X POST http://localhost:8083/api/upload \
  -F "file=@datasource-api/nyc_taxi_sample.csv" \
  -F "table_name=nyc_taxi"


# Get data
curl "http://localhost:8083/api/data/nyc_taxi?page=1&page_size=10"
```

## 📊 NYC Taxi Data Schema

Dữ liệu NYC Taxi có các cột sau:

| Column                | Type       | Description            |
| --------------------- | ---------- | ---------------------- |
| VendorID              | INTEGER    | Vendor identifier      |
| tpep_pickup_datetime  | TIMESTAMP  | Pickup datetime        |
| tpep_dropoff_datetime | TIMESTAMP  | Dropoff datetime       |
| passenger_count       | INTEGER    | Number of passengers   |
| trip_distance         | NUMERIC    | Trip distance in miles |
| RatecodeID            | INTEGER    | Rate code              |
| store_and_fwd_flag    | VARCHAR(1) | Y/N flag               |
| PULocationID          | INTEGER    | Pickup location ID     |
| DOLocationID          | INTEGER    | Dropoff location ID    |
| payment_type          | INTEGER    | Payment type           |
| fare_amount           | NUMERIC    | Fare amount            |
| extra                 | NUMERIC    | Extra charges          |
| mta_tax               | NUMERIC    | MTA tax                |
| tip_amount            | NUMERIC    | Tip amount             |
| tolls_amount          | NUMERIC    | Tolls amount           |
| improvement_surcharge | NUMERIC    | Improvement surcharge  |
| total_amount          | NUMERIC    | Total amount           |
| congestion_surcharge  | NUMERIC    | Congestion surcharge   |

## 📚 API Endpoints

### 1. Health Check

```bash
GET http://localhost:8083/health
```

### 2. Upload CSV

```bash
POST http://localhost:8083/api/upload

# Example
curl -X POST http://localhost:8083/api/upload \
  -F "file=@nyc_taxi_data.csv" \
  -F "table_name=nyc_taxi"
```

**Response:**

```json
{
  "status": "success",
  "table_name": "nyc_taxi",
  "rows_inserted": 7,
  "columns": ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", ...],
  "timestamp": "2024-11-18T10:00:00"
}
```

### 3. List Tables

```bash
GET http://localhost:8083/api/tables

curl http://localhost:8083/api/tables
```

### 4. Get Data (with Pagination)

```bash
GET http://localhost:8083/api/data/{table_name}?page=1&page_size=100

curl "http://localhost:8083/api/data/nyc_taxi?page=1&page_size=10"
```

**Response:**

```json
{
  "status": "success",
  "table_name": "nyc_taxi",
  "pagination": {
    "page": 1,
    "page_size": 10,
    "total_records": 7,
    "total_pages": 1,
    "has_next": false,
    "has_previous": false
  },
  "data": [
    {
      "vendorid": 1,
      "tpep_pickup_datetime": "2020-04-01T00:41:22",
      "tpep_dropoff_datetime": "2020-04-01T01:01:53",
      "passenger_count": 1,
      "trip_distance": "1.20",
      "fare_amount": "5.50",
      ...
    }
  ]
}
```

## 🔧 NiFi Configuration

### InvokeHTTP Processor

1. **Remote URL**: `http://datasource-api:8000/api/data/nyc_taxi?page=${page:or(1)}&page_size=100`
2. **HTTP Method**: GET
3. **Success Status Codes**: 200

### Extract Data from JSON

```json
$.data[*]
```

### Pagination Flow

```
InvokeHTTP → EvaluateJsonPath (extract pagination) → RouteOnAttribute → Loop back if has_next
```

## 🐳 Docker Commands

```bash
# Start
docker-compose -f docker-compose-datasource.yaml up -d --build

# Stop
docker-compose -f docker-compose-datasource.yaml down

# Logs
docker logs -f datasource-api
docker logs -f datasource-db

# Access database
docker exec -it datasource-db psql -U datasource_admin -d datasource_db

# Reset (delete all data)
docker-compose -f docker-compose-datasource.yaml down -v
docker-compose -f docker-compose-datasource.yaml up -d --build
```

## 🧪 Testing

### Test với sample data

```bash
# Upload sample NYC Taxi CSV
curl -X POST http://localhost:8083/api/upload \
  -F "file=@datasource-api/nyc_taxi_sample.csv" \
  -F "table_name=nyc_taxi"

# List tables
curl http://localhost:8083/api/tables

# Get first page
curl "http://localhost:8083/api/data/nyc_taxi?page=1&page_size=5"

# Query from PostgreSQL directly
docker exec -it datasource-db psql -U datasource_admin -d datasource_db \
  -c "SELECT vendorid, tpep_pickup_datetime, fare_amount FROM nyc_taxi LIMIT 5;"
```

### Test với Python

```python
import requests
import pandas as pd

# Upload CSV
with open('nyc_taxi_data.csv', 'rb') as f:
    response = requests.post(
        'http://localhost:8083/api/upload',
        files={'file': f},
        data={'table_name': 'nyc_taxi'}
    )
    print(response.json())

# Get data
response = requests.get(
    'http://localhost:8083/api/data/nyc_taxi',
    params={'page': 1, 'page_size': 100}
)
data = response.json()

# Convert to DataFrame
df = pd.DataFrame(data['data'])
print(df.head())
print(f"Total records: {data['pagination']['total_records']}")
```

## 📝 Notes

- API tự động detect và parse datetime columns
- Column names được sanitize (lowercase, remove spaces)
- Numeric columns (fare_amount, tip_amount, etc.) được lưu dạng NUMERIC trong PostgreSQL
- Database đã có sample data table `nyc_taxi_sample` với 7 records để test ngay

## 🏗️ Architecture

```
CSV Upload → FastAPI → PostgreSQL (datasource-db:5435)
                ↓
        NiFi InvokeHTTP ← REST API (JSON)
                ↓
        Process & Load → Iceberg/MinIO
```

## 🔍 Troubleshooting

### Check if services are running

```bash
docker ps | grep datasource
```

### Check database tables

```bash
docker exec -it datasource-db psql -U datasource_admin -d datasource_db -c "\dt"
```

### View table schema

```bash
docker exec -it datasource-db psql -U datasource_admin -d datasource_db \
  -c "\d nyc_taxi"
```

### Check API logs

```bash
docker logs -f datasource-api
```

# Webserver

docker exec -it airflow-webserver pip install "apache-airflow-providers-apache-kafka==1.4.0"

# Scheduler (cần cùng phiên bản)

docker exec -it airflow-scheduler pip install "apache-airflow-providers-apache-kafka==1.4.0"

# Worker (nếu bạn chạy CeleryExecutor/K8sExecutor)

docker exec -it airflow-worker pip install "apache-airflow-providers-apache-kafka==1.4.0"
