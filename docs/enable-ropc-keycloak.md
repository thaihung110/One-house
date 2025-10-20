# Enable ROPC Grant trong Keycloak

## Bước 1: Enable Direct Access Grants cho Client "trino"

### Via Keycloak Admin UI:

1. Login vào Keycloak Admin: http://localhost:30080/admin

   - Username: `admin`
   - Password: `admin`

2. Select realm: `iceberg`

3. Go to **Clients** → Click **trino**

4. Settings tab:

   - ✅ Enable **Direct Access Grants** (Resource Owner Password Credentials)
   - ✅ Enable **Standard Flow** (giữ nguyên cho browser flow)
   - ✅ Enable **Service Accounts** (optional, cho client credentials)

5. Click **Save**

## Bước 2: Via kcadm CLI (Automated)

```bash
# Login to kcadm
docker exec keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 \
  --realm master \
  --user admin \
  --password admin

# Enable Direct Access Grants for trino client
docker exec keycloak /opt/keycloak/bin/kcadm.sh update clients/<client-id> \
  -r iceberg \
  -s 'directAccessGrantsEnabled=true' \
  -s 'standardFlowEnabled=true' \
  -s 'serviceAccountsEnabled=true'
```

## Bước 3: Test ROPC Grant

```bash
# Test get token with username/password
curl -X POST http://localhost:30080/realms/iceberg/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=trino" \
  -d "client_secret=AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ" \
  -d "username=hung" \
  -d "password=iceberg"
```

Expected response:

```json
{
  "access_token": "eyJhbGc...",
  "expires_in": 300,
  "refresh_expires_in": 1800,
  "refresh_token": "eyJhbGc...",
  "token_type": "Bearer",
  "id_token": "eyJhbGc...",
  "not-before-policy": 0,
  "session_state": "...",
  "scope": "openid email profile"
}
```

## Bước 4: Update realm.json (Persistent Config)

```json
{
  "clients": [
    {
      "clientId": "trino",
      "directAccessGrantsEnabled": true,
      "standardFlowEnabled": true,
      "serviceAccountsEnabled": true,
      "publicClient": false,
      "secret": "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ"
    }
  ]
}
```

## Security Notes

⚠️ **ROPC Grant chỉ nên dùng cho:**

- Internal applications
- Trusted first-party apps
- Development/testing environments
- Legacy apps migration

❌ **KHÔNG nên dùng cho:**

- Third-party applications
- Production public apps
- Untrusted clients
