#!/usr/bin/env python3
"""
Quick DataGrip Connection Helper
Auto-generates JDBC URL with fresh token
"""
import requests

KEYCLOAK_TOKEN_URL = (
    "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"
)

print("=" * 80)
print("DATAGRIP QUICK CONNECT - TRINO")
print("=" * 80)

# Get token
response = requests.post(
    KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "password",
        "client_id": "trino",
        "client_secret": "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ",
        "username": "hung",
        "password": "hung",
        "scope": "trino",
    },
)

if response.status_code != 200:
    print(f"ERROR: Cannot get token - {response.status_code}")
    print(response.text)
    exit(1)

token = response.json()["access_token"]
expires_in = response.json()["expires_in"]

jdbc_url = f"jdbc:trino://localhost:443?accessToken={token}"

print(f"\nToken expires in: {expires_in // 60} minutes")
print("\n" + "=" * 80)
print("COPY THIS URL TO DATAGRIP")
print("=" * 80)
print(f"\n{jdbc_url}\n")

print("=" * 80)
print("DATAGRIP CONFIGURATION STEPS")
print("=" * 80)
print(
    """
    1. GENERAL TAB:
    - Name: @localhost
    - Host: localhost
    - Port: 443
    - Database: (leave empty or use 'lake_bronze')
    
    URL: (paste URL above)

    2. AUTHENTICATION:
    - Type: No auth
    
    3. SSH/SSL TAB:
    ✅ Use SSL: CHECKED
    - Mode: Require (no verification)
    - CA file: (leave empty)
    - Use truststore: ☑ IDE  ☑ Java  ☑ System

    4. Click "Test Connection"

    5. Should see: ✅ "Connection successful"
"""
)

print("=" * 80)
print("TIPS")
print("=" * 80)
print(
    """
- Token expires in 60 minutes
- When expired, just run this script again and replace URL
- No need to restart DataGrip
- Copy URL directly from above output
"""
)

# Try to copy to clipboard
try:
    import pyperclip

    pyperclip.copy(jdbc_url)
    print("\n✅ URL copied to clipboard! Just paste it in DataGrip.")
except ImportError:
    print("\n💡 Tip: Install pyperclip to auto-copy URL to clipboard:")
    print("   pip install pyperclip")
