import json
import logging

import pandas as pd
import pyspark
import requests
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CATALOG_URL = "http://lakekeeper:8181/catalog"
WAREHOUSE = "bronze"

KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)
KEYCLOAK_REGISTRATION_URL = (
    "http://keycloak:8080/realms/iceberg/clients-registrations/default"
)
MANAGEMENT_URL = "http://lakekeeper:8181/management"

ADMIN_USER = "admin-new"
ADMIN_PASS = "admin"
CLIENT_ID = "lakekeeper"


def register_spark_client(app_client_id: str) -> tuple[str, str]:
    """
    Register spark client in lakekeeper
    """

    # 1. Get admin token
    resp = requests.post(
        url=KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "password",
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
            "client_id": CLIENT_ID,
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    admin_token = resp.json()["access_token"]

    # 2. Check if client exists in Keycloak
    response = requests.get(
        url=f"{KEYCLOAK_REGISTRATION_URL}/{app_client_id}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {admin_token}",
        },
    )

    if response.status_code == 200:
        logger.info(f"Client {app_client_id} already exists")
        app_client_secret = response.json()["secret"]
    else:
        # If not, register new client
        response_post = requests.post(
            url=KEYCLOAK_REGISTRATION_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {admin_token}",
            },
            json={
                "clientId": f"{app_client_id}",
                "protocol": "openid-connect",
                "publicClient": False,
                "serviceAccountsEnabled": True,
            },
        )
        response_post.raise_for_status()
        app_client_secret = response_post.json()["secret"]

    # 3. Get access token for this client
    response = requests.post(
        url=KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": app_client_id,
            "client_secret": app_client_secret,
            "scope": "lakekeeper",
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    access_token_client = response.json()["access_token"]

    # 4. Create machine user in Lakekeeper
    response = requests.post(
        url=f"{MANAGEMENT_URL}/v1/user",
        headers={"Authorization": f"Bearer {access_token_client}"},
        json={"update-if-exists": True},
    )
    response.raise_for_status()

    # 5. Get list of users from Lakekeeper to find client_id
    response_get_user = requests.get(
        url=f"{MANAGEMENT_URL}/v1/user",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    response_get_user.raise_for_status()
    users = response_get_user.json().get("users", [])
    matched_user = next(
        (u for u in users if app_client_id in u.get("name", "")), None
    )
    if not matched_user:
        raise RuntimeError(f"No user found for client {app_client_id}")

    client_id = matched_user.get("id")

    # 6. Check if client has project_admin role
    response_get = requests.get(
        url=f"{MANAGEMENT_URL}/v1/permissions/project/assignments",
        headers={
            "Authorization": f"Bearer {admin_token}",
            "Accept": "application/json",
        },
    )
    response_get.raise_for_status()
    assignments = response_get.json().get("assignments", [])
    already_assigned = any(
        a.get("type") == "project_admin" and a.get("user") == client_id
        for a in assignments
    )

    if not already_assigned:
        response_post = requests.post(
            url=f"{MANAGEMENT_URL}/v1/permissions/project/assignments",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "writes": [
                    {
                        "type": "project_admin",
                        "user": f"{client_id}",
                    }
                ]
            },
        )
        response_post.raise_for_status()
        logger.info(f"Assigned project_admin role for client {app_client_id}")

    return app_client_id, app_client_secret
