import json

import jwt
import requests

KEYCLOAK_TOKEN_URL = (
    "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
)
MANAGEMENT_URL = "http://lakekeeper:8181/management"

# 1. get token with user admin-new and client lakekeeper
response = requests.post(
    url=KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "password",
        "client_id": "lakekeeper",
        "username": "admin-new",
        "password": "admin",
    },
    headers={"Content-type": "application/x-www-form-urlencoded"},
)
response.raise_for_status()

access_token = response.json()["access_token"]
print("Access Token:")
print(access_token)

# 2. Inspect token to see payload
decoded = jwt.decode(access_token, options={"verify_signature": False})
print("Decoded token:")
print(json.dumps(decoded, indent=2))

# 3. Check bootstrap status
resp_info = requests.get(
    url=f"{MANAGEMENT_URL}/v1/info",
    headers={"Authorization": f"Bearer {access_token}"},
)
resp_info.raise_for_status()

info = resp_info.json()
print("Lakekeeper info:")
print(json.dumps(info, indent=2))

if not info.get("bootstrapped", False):
    print("Lakekeeper not bootstrapped, bootstrapping...")

    resp_bootstrap = requests.post(
        url=f"{MANAGEMENT_URL}/v1/bootstrap",
        headers={"Authorization": f"Bearer {access_token}"},
        json={"accept-terms-of-use": True},
    )
    resp_bootstrap.raise_for_status()

    print("Bootstrap response:")
    print(json.dumps(resp_bootstrap.json(), indent=2))
else:
    print("Lakekeeper already bootstrapped ✅")
