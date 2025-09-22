#!/bin/bash
set -e

cd /opt/keycloak/bin

login_admin() {
  local server_url=$1
  local admin_user=$2
  local admin_pass=$3

  echo "👉 Logging in as Keycloak admin..."
  ./kcadm.sh config credentials \
    --server "$server_url" \
    --realm master \
    --user "$admin_user" \
    --password "$admin_pass"
}

get_user_id() {
  local realm=$1
  local username=$2

  ./kcadm.sh get users -r "$realm" -q username="$username" \
    --fields id --format csv | tail -n +2 | tr -d '"'
}

create_user() {
  local realm=$1
  local username=$2
  local password=$3

  echo "👉 User $username does not exist, creating new user..."
  ./kcadm.sh create users -r "$realm" \
    -s username="$username" \
    -s enabled=true

  ./kcadm.sh set-password -r "$realm" \
    --username "$username" \
    --new-password "$password" \
    --temporary=false

  echo "✅ User $username has been created with password $password"
}

assign_realm_admin_role() {
  local realm=$1
  local username=$2

  echo "👉 Assigning role realm-admin to $username ..."
  ./kcadm.sh add-roles -r "$realm" \
    --uusername "$username" \
    --cclientid realm-management \
    --rolename realm-admin

  echo "✅ Completed assigning realm-admin role to $username"
}

main() {
  local server_url=$1
  local realm=$2
  local admin_cli_user=$3
  local admin_cli_pass=$4
  local new_user=$5
  local new_pass=$6

  login_admin "$server_url" "$admin_cli_user" "$admin_cli_pass"

  USER_ID=$(get_user_id "$realm" "$new_user")

  if [ -z "$USER_ID" ]; then
    create_user "$realm" "$new_user" "$new_pass"
  else
    echo "ℹ️ User $new_user already exists (id=$USER_ID)"
  fi

  assign_realm_admin_role "$realm" "$new_user"
  echo "✅ Completed creating admin for realm $realm"
}

# Run script
main "$@"
