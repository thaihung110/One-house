# Permission check logic against RBAC API

package rbac

import rego.v1

import data.rbac

# Check if user has permission to perform operation on resource
# Returns true if allowed, false otherwise
check_permission(user_id, resource, operation) := allowed if {
    response := rbac.call_permission_check(user_id, resource, operation)
    response.status_code == 200
    allowed := response.body.allowed
}

# Default to false if API call fails or returns non-200
check_permission(user_id, resource, operation) := false if {
    response := rbac.call_permission_check(user_id, resource, operation)
    response.status_code != 200
}

# Default to false if response doesn't contain allowed field
check_permission(user_id, resource, operation) := false if {
    response := rbac.call_permission_check(user_id, resource, operation)
    response.status_code == 200
    not response.body.allowed
}

