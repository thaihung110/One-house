# Configuration for RBAC API integration

package configuration

import rego.v1

# Get environment variables
env := opa.runtime().env

# RBAC API Configuration
# The URL where OPA can reach the RBAC API
rbac_api_url := trim_right(object.get(env, "RBAC_API_URL", "http://rbac-api:8000"), "/")

# HTTP timeout for RBAC API calls (in seconds)
rbac_api_timeout := object.get(env, "RBAC_API_TIMEOUT", "2s")

# Enable debug logging (set to "true" to enable)
debug_enabled := object.get(env, "RBAC_DEBUG", "false") == "true"

