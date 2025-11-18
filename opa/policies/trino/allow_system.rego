# System catalog and special authorization rules

package trino

import rego.v1

import data.rbac
import data.trino

# ============================================================================
# SYSTEM CATALOG SPECIAL RULES
# ============================================================================

# Allow access to system catalog for users with any permissions
allow_system if {
    input.action.operation in ["AccessCatalog", "FilterCatalogs"]
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    # Check if user has any permissions (ExecuteQuery check)
    rbac.check_permission(trino.user_id, {}, "ExecuteQuery")
}

# Allow showing schemas in system catalog
allow_system if {
    input.action.operation in ["ShowSchemas", "FilterSchemas"]
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    rbac.check_permission(trino.user_id, {}, "ExecuteQuery")
}

# Allow querying system catalog tables
allow_system if {
    input.action.operation == "SelectFromColumns"
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    rbac.check_permission(trino.user_id, {}, "ExecuteQuery")
}

# ============================================================================
# INFORMATION_SCHEMA SPECIAL RULES
# ============================================================================

# Allow information_schema access in any catalog
allow_system if {
    input.action.operation == "FilterSchemas"
    schema_name := trino.get_schema_name(input.action.resource)
    schema_name == "information_schema"
    rbac.check_permission(trino.user_id, {}, "ExecuteQuery")
}

# Allow querying information_schema tables
allow_system if {
    input.action.operation == "SelectFromColumns"
    schema_name := trino.get_schema_name(input.action.resource)
    schema_name == "information_schema"
    catalog_name := trino.get_catalog_name(input.action.resource)
    # Check if user has access to this catalog
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "AccessCatalog")
}

# ============================================================================
# GENERAL RULES
# ============================================================================

# ExecuteQuery - allow for any authenticated user (no RBAC lookup)
allow_system if {
    input.action.operation == "ExecuteQuery"
    trino.user_id
}

