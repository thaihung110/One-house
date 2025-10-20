#!/bin/bash
# Script để enable Resource Owner Password Credentials Grant trong Keycloak
# Cho phép authenticate bằng username/password trực tiếp trong code

set -e

echo "=================================================="
echo "Enable ROPC Grant for Trino Client in Keycloak"
echo "=================================================="

KEYCLOAK_ADMIN_URL="http://localhost:30080"
REALM="iceberg"
ADMIN_USER="admin"
ADMIN_PASSWORD="admin"
CLIENT_ID="trino"

echo ""
echo "Step 1: Get admin access token..."
ADMIN_TOKEN=$(curl -s -X POST "${KEYCLOAK_ADMIN_URL}/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${ADMIN_USER}" \
  -d "password=${ADMIN_PASSWORD}" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" | jq -r '.access_token')

if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
    echo "❌ Failed to get admin token"
    exit 1
fi

echo "✅ Admin token obtained"

echo ""
echo "Step 2: Get trino client UUID..."
CLIENT_UUID=$(curl -s -X GET "${KEYCLOAK_ADMIN_URL}/admin/realms/${REALM}/clients" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" | jq -r ".[] | select(.clientId==\"${CLIENT_ID}\") | .id")

if [ -z "$CLIENT_UUID" ] || [ "$CLIENT_UUID" = "null" ]; then
    echo "❌ Client '${CLIENT_ID}' not found"
    exit 1
fi

echo "✅ Client UUID: ${CLIENT_UUID}"

echo ""
echo "Step 3: Update client to enable Direct Access Grants (ROPC)..."
curl -s -X PUT "${KEYCLOAK_ADMIN_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "directAccessGrantsEnabled": true,
    "standardFlowEnabled": true,
    "serviceAccountsEnabled": true
  }'

echo "✅ Client updated successfully"

echo ""
echo "Step 4: Verify configuration..."
CLIENT_CONFIG=$(curl -s -X GET "${KEYCLOAK_ADMIN_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}")

DIRECT_ACCESS=$(echo "$CLIENT_CONFIG" | jq -r '.directAccessGrantsEnabled')
STANDARD_FLOW=$(echo "$CLIENT_CONFIG" | jq -r '.standardFlowEnabled')
SERVICE_ACCOUNT=$(echo "$CLIENT_CONFIG" | jq -r '.serviceAccountsEnabled')

echo "  - Direct Access Grants (ROPC): ${DIRECT_ACCESS}"
echo "  - Standard Flow (Browser): ${STANDARD_FLOW}"
echo "  - Service Accounts: ${SERVICE_ACCOUNT}"

echo ""
echo "Step 5: Test ROPC grant with user credentials..."
TEST_TOKEN=$(curl -s -X POST "${KEYCLOAK_ADMIN_URL}/realms/${REALM}/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=${CLIENT_ID}" \
  -d "client_secret=AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ" \
  -d "username=hung" \
  -d "password=iceberg" | jq -r '.access_token')

if [ -z "$TEST_TOKEN" ] || [ "$TEST_TOKEN" = "null" ]; then
    echo "❌ ROPC test failed"
    exit 1
fi

echo "✅ ROPC test successful! Token obtained:"
echo "${TEST_TOKEN:0:50}..."

echo ""
echo "=================================================="
echo "✅ ROPC Grant enabled successfully!"
echo "=================================================="
echo ""
echo "You can now use username/password in your code:"
echo ""
echo "  python notebooks/trino_ropc_auth.py"
echo ""
echo "Or use the smart connection wrapper:"
echo ""
echo "  python notebooks/trino_smart_connection.py"
echo ""


