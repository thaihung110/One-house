# System catalog and special authorization rules

package trino

import rego.v1

import data.rbac
import data.trino

# ============================================================================
# GENERAL RULES - ExecuteQuery (evaluated first)
# ============================================================================

# ExecuteQuery - always allow (no checks required)
# This rule allows ExecuteQuery operations without any checks
allow_system if {
    input.action.operation == "ExecuteQuery"
}

# ============================================================================
# SYSTEM CATALOG SPECIAL RULES
# ============================================================================

# Allow access to system catalog for users with any permissions
allow_system if {
    input.action.operation in ["AccessCatalog", "FilterCatalogs"]
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
}

# Allow showing schemas in system catalog
allow_system if {
    input.action.operation in ["ShowSchemas", "FilterSchemas"]
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
}

# Allow querying system catalog tables
# Always allow for IDE/metadata access (e.g., PyCharm, JDBC metadata).
# This bypasses RBAC for `system` catalog table reads.
allow_system if {
    input.action.operation == "SelectFromColumns"
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName in ["catalogs", "types"]
}

allow_system if {
    input.action.operation == "SelectFromColumns"
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName == "tables"
}

allow_system if {
    input.action.operation == "SelectFromColumns"
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName == "columns"
}

allow_system if {
    input.action.operation == "SelectFromColumns"
    catalog_name := trino.get_catalog_name(input.action.resource)
    catalog_name == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName == "schemas"
}

# ============================================================================
# INFORMATION_SCHEMA SPECIAL RULES
# ============================================================================

# Always allow any query targeting information_schema (bypass RBAC)
allow_system if {
    schema_name := trino.get_schema_name(input.action.resource)
    schema_name == "information_schema"
}

# Allow SelectFromColumns on key information_schema tables
allow_system if {
    input.action.operation == "SelectFromColumns"
    input.action.resource.table.schemaName == "information_schema"
    input.action.resource.table.tableName in ["schemata", "tables", "columns", "views"]
}

# Always allow FilterSchemas for information_schema
allow_system if {
    input.action.operation == "FilterSchemas"
    schema := input.action.resource.schema.schemaName
    schema == "information_schema"
}

# Allow FilterTables when user has describe permission on namespace/schema
# This is stricter than catalog-level access - requires schema-level permission
allow_system if {
    input.action.operation == "FilterTables"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "FilterTables")
}
