# HTTP authentication and communication with Permission API

package rbac

import rego.v1

import data.configuration

# Call Permission API permission check endpoint
# Returns the HTTP response object
call_permission_check(user_id, resource, operation) := response if {
    response := http.send({
        "method": "POST",
        "url": sprintf("%s/api/v1/permissions/check", [configuration.permission_api_url]),
        "headers": {"Content-Type": "application/json"},
        "body": {
            "user_id": user_id,
            "resource": resource,
            "operation": operation,
        },
        "timeout": configuration.permission_api_timeout,
        "raise_error": false,
    })
}

