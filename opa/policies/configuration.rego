# Configuration for RBAC API integration

package configuration

import rego.v1

# Get environment variables
env := opa.runtime().env

# Permission API Configuration
# The URL where OPA can reach the Permission API
permission_api_url := trim_right(object.get(env, "PERMISSION_API_URL", "http://permission-api:8000"), "/")

# HTTP timeout for Permission API calls (in seconds)
permission_api_timeout := object.get(env, "PERMISSION_API_TIMEOUT", "2s")

# Enable debug logging (set to "true" to enable)
debug_enabled := object.get(env, "PERMISSION_DEBUG", "false") == "true"

