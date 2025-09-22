import json
import os

import boto3
import requests

# ==========================
# Config
# ==========================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)
MANAGEMENT_URL = "http://lakekeeper:8181/management"

ADMIN_USER = "admin-new"
ADMIN_PASS = "admin"
CLIENT_ID = "lakekeeper"

# ==========================
# Step 1. Get Keycloak token
# ==========================
resp = requests.post(
    url=KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
    },
    headers={"Content-type": "application/x-www-form-urlencoded"},
)
resp.raise_for_status()
access_token = resp.json()["access_token"]

print("✅ Got Keycloak access token {access_token}")

# ==========================
# Step 2. List buckets in MinIO
# ==========================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="local-01",
)

buckets = s3.list_buckets()["Buckets"]
print("📂 Buckets in MinIO:")
for b in buckets:
    print(" -", b["Name"])

# ==========================
# Step 3. Create warehouse in Lakekeeper for each bucket
# ==========================
for b in buckets:
    bucket_name = b["Name"]
    warehouse_name = f"{bucket_name}"

    payload = {
        "warehouse-name": warehouse_name,
        "storage-profile": {
            "type": "s3",
            "bucket": bucket_name,
            "key-prefix": "warehouse",
            "endpoint": MINIO_ENDPOINT,
            "region": "local-01",
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": True,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": MINIO_ACCESS_KEY,
            "aws-secret-access-key": MINIO_SECRET_KEY,
        },
    }

    resp = requests.post(
        url=f"{MANAGEMENT_URL}/v1/warehouse",
        headers={"Authorization": f"Bearer {access_token}"},
        json=payload,
    )

    try:
        resp.raise_for_status()
        print(f"✅ Created warehouse {warehouse_name} for bucket {bucket_name}")
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(f"❌ Failed to create warehouse {warehouse_name}: {resp.text}")
