#!/bin/bash
set -euo pipefail

# Chạy Keycloak với args từ command trong compose
/opt/keycloak/bin/kc.sh "$@" &
KC_PID=$!


# Chạy script tạo admin trong realm iceberg
/opt/keycloak/create-iceberg-admin.sh \
  http://localhost:8080 \
  iceberg \
  "$KC_BOOTSTRAP_ADMIN_USERNAME" \
  "$KC_BOOTSTRAP_ADMIN_PASSWORD" \
  admin-new \
  admin || true

wait $KC_PID
