# User ID extraction from Trino input

package trino

import rego.v1

# Extract user ID from Trino request context
user_id := input.context.identity.user

