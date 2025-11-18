# HTTP authentication and communication with RBAC API

package rbac

import rego.v1

import data.configuration

# Call RBAC API permission check endpoint
# Returns the HTTP response object
call_permission_check(user_id, resource, operation) := response if {
    response := http.send({
        "method": "POST",
        "url": sprintf("%s/permissions/check", [configuration.rbac_api_url]),
        "headers": {"Content-Type": "application/json"},
        "body": {
            "user_id": user_id,
            "resource": resource,
            "operation": operation,
        },
        "timeout": configuration.rbac_api_timeout,
        "raise_error": false,
    })
}

