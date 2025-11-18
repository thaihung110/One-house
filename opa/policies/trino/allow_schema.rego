# Schema-level authorization rules

package trino

import rego.v1

import data.rbac
import data.trino

# ============================================================================
# SCHEMA-LEVEL OPERATIONS
# ============================================================================

# ShowSchemas - user needs SHOW_SCHEMAS privilege
allow_schema if {
    input.action.operation == "ShowSchemas"
    catalog_name := trino.get_catalog_name(input.action.resource)
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "ShowSchemas")
}

# CreateSchema - user needs CREATE_SCHEMA privilege
allow_schema if {
    input.action.operation == "CreateSchema"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "CreateSchema")
}

# DropSchema - user needs DROP_SCHEMA privilege
allow_schema if {
    input.action.operation == "DropSchema"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "DropSchema")
}

# RenameSchema - user needs DROP_SCHEMA privilege
allow_schema if {
    input.action.operation == "RenameSchema"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "RenameSchema")
}

# SetSchemaAuthorization - user needs DROP_SCHEMA privilege
allow_schema if {
    input.action.operation == "SetSchemaAuthorization"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "SetSchemaAuthorization")
}

# FilterSchemas - filter schemas based on user permissions
allow_schema if {
    input.action.operation == "FilterSchemas"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "FilterSchemas")
}

