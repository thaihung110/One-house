package authz

import rego.v1

default allow := false

# Fetch user policies from RBAC API
user_policies := response.body if {
    response := http.send({
        "method": "GET",
        "url": sprintf("http://rbac-api:8000/users/%s/policies", [input.context.identity.user]),
        "timeout": "2s",
        "raise_error": false
    })
    response.status_code == 200
}

# Default to empty array if fetch fails
user_policies := [] if {
    response := http.send({
        "method": "GET",
        "url": sprintf("http://rbac-api:8000/users/%s/policies", [input.context.identity.user]),
        "timeout": "2s",
        "raise_error": false
    })
    response.status_code != 200
}

#------------------------------------------------------------------------------
# HELPER FUNCTIONS
#------------------------------------------------------------------------------

# Check if field matches (null or empty string = wildcard)
field_matches(policy_value, input_value) if {
    policy_value == null
}

field_matches(policy_value, input_value) if {
    policy_value == ""
}

field_matches(policy_value, input_value) if {
    policy_value == input_value
}

# Check if user has required privilege
has_privilege(policy, required_privilege) if {
    some action in policy.actions
    action == "ALL"
}

has_privilege(policy, required_privilege) if {
    some action in policy.actions
    action == required_privilege
}

#------------------------------------------------------------------------------
# SYSTEM-LEVEL OPERATIONS
#------------------------------------------------------------------------------

# Allow ExecuteQuery for any authenticated user with policies
allow if {
    input.action.operation == "ExecuteQuery"
    count(user_policies) > 0
}

# Allow ImpersonateUser if user has ALL privilege
allow if {
    input.action.operation == "ImpersonateUser"
    some policy in user_policies
    has_privilege(policy, "ALL")
}

#------------------------------------------------------------------------------
# CATALOG-LEVEL OPERATIONS
#------------------------------------------------------------------------------

# ShowCatalogs - allow if user has any policies
allow if {
    input.action.operation == "ShowCatalogs"
    count(user_policies) > 0
}

# FilterCatalogs - filter catalogs based on user permissions
# This is called for each catalog when doing SHOW CATALOGS
allow if {
    input.action.operation == "FilterCatalogs"
    catalog_name := input.action.resource.catalog.name
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
}

# AccessCatalog - check catalog permission
allow if {
    input.action.operation == "AccessCatalog"
    catalog_name := input.action.resource.catalog.name
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    has_privilege(policy, "ALL")
}

# CreateCatalog - requires CREATE_CATALOG or ALL privilege
allow if {
    input.action.operation == "CreateCatalog"
    some policy in user_policies
    has_privilege(policy, "CREATE_CATALOG")
}

allow if {
    input.action.operation == "CreateCatalog"
    some policy in user_policies
    has_privilege(policy, "ALL")
}

# DropCatalog - requires DROP_CATALOG or ALL privilege
allow if {
    input.action.operation == "DropCatalog"
    catalog_name := input.action.resource.catalog.name
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    has_privilege(policy, "DROP_CATALOG")
}

#------------------------------------------------------------------------------
# SCHEMA-LEVEL OPERATIONS
#------------------------------------------------------------------------------

# ShowSchemas - check catalog permission
allow if {
    input.action.operation == "ShowSchemas"
    catalog_name := input.action.resource.schema.catalogName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
}

# ShowSchemas - alternative input shape where catalog is provided directly
allow if {
    input.action.operation == "ShowSchemas"
    catalog_name := input.action.resource.catalog.name
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
}

# FilterSchemas - filter schemas based on user permissions
allow if {
    input.action.operation == "FilterSchemas"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
}

# CreateSchema - requires CREATE_SCHEMA or ALL privilege
allow if {
    input.action.operation == "CreateSchema"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    has_privilege(policy, "CREATE_SCHEMA")
}

# DropSchema - requires DROP_SCHEMA or ALL privilege
allow if {
    input.action.operation == "DropSchema"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    has_privilege(policy, "DROP_SCHEMA")
}

# RenameSchema - requires DROP_SCHEMA or ALL privilege
allow if {
    input.action.operation == "RenameSchema"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    has_privilege(policy, "DROP_SCHEMA")
}

# SetSchemaAuthorization - requires DROP_SCHEMA or ALL privilege
allow if {
    input.action.operation == "SetSchemaAuthorization"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    has_privilege(policy, "DROP_SCHEMA")
}

#------------------------------------------------------------------------------
# TABLE-LEVEL OPERATIONS
#------------------------------------------------------------------------------

# ShowTables - check catalog and schema permission
allow if {
    input.action.operation == "ShowTables"
    catalog_name := input.action.resource.schema.catalogName
    schema_name := input.action.resource.schema.schemaName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
}

# ShowColumns - check table permission
allow if {
    input.action.operation == "ShowColumns"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
}

# CreateTable - requires CREATE_TABLE or ALL privilege
allow if {
    input.action.operation == "CreateTable"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "CREATE_TABLE")
}

# DropTable - requires DROP_TABLE or ALL privilege
allow if {
    input.action.operation == "DropTable"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DROP_TABLE")
}

# RenameTable - requires DROP_TABLE or ALL privilege
allow if {
    input.action.operation == "RenameTable"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DROP_TABLE")
}

# SetTableComment - requires UPDATE_TABLE or ALL privilege
allow if {
    input.action.operation == "SetTableComment"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "UPDATE_TABLE")
}

# SetTableAuthorization - requires DROP_TABLE or ALL privilege
allow if {
    input.action.operation == "SetTableAuthorization"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DROP_TABLE")
}

#------------------------------------------------------------------------------
# DATA OPERATIONS
#------------------------------------------------------------------------------

# SelectFromColumns - requires SELECT_TABLE or ALL privilege
# Also check column-level permissions if specified
allow if {
    input.action.operation == "SelectFromColumns"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "SELECT_TABLE")
    columns_allowed(input.action.resource.table.columns, policy.columns)
}

# Helper: check if requested columns are allowed
columns_allowed(requested_columns, allowed_columns) if {
    allowed_columns == null  # null means all columns allowed
}

columns_allowed(requested_columns, allowed_columns) if {
    requested_columns == null  # null means selecting all columns
    allowed_columns == null
}

columns_allowed(requested_columns, allowed_columns) if {
    count(requested_columns) == 0  # empty list
}

columns_allowed(requested_columns, allowed_columns) if {
    allowed_columns != null
    count(requested_columns) > 0
    # Check all requested columns are in allowed list
    count([col | some col in requested_columns; col in allowed_columns]) == count(requested_columns)
}

# InsertIntoTable - requires INSERT_TABLE or ALL privilege
allow if {
    input.action.operation == "InsertIntoTable"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "INSERT_TABLE")
}

# DeleteFromTable - requires DELETE_TABLE or ALL privilege
allow if {
    input.action.operation == "DeleteFromTable"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DELETE_TABLE")
}

# UpdateTableColumns - requires UPDATE_TABLE or ALL privilege
allow if {
    input.action.operation == "UpdateTableColumns"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "UPDATE_TABLE")
}

#------------------------------------------------------------------------------
# COLUMN OPERATIONS
#------------------------------------------------------------------------------

# AddColumn, DropColumn, RenameColumn, SetColumnComment - requires UPDATE_TABLE or ALL
allow if {
    input.action.operation in ["AddColumn", "DropColumn", "RenameColumn", "SetColumnComment"]
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "UPDATE_TABLE")
}

#------------------------------------------------------------------------------
# VIEW OPERATIONS
#------------------------------------------------------------------------------

# CreateView, DropView - same as table operations
allow if {
    input.action.operation == "CreateView"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "CREATE_TABLE")
}

allow if {
    input.action.operation == "DropView"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DROP_TABLE")
}

#------------------------------------------------------------------------------
# MATERIALIZED VIEW OPERATIONS
#------------------------------------------------------------------------------

allow if {
    input.action.operation == "CreateMaterializedView"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "CREATE_TABLE")
}

allow if {
    input.action.operation == "DropMaterializedView"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "DROP_TABLE")
}

allow if {
    input.action.operation == "RefreshMaterializedView"
    catalog_name := input.action.resource.table.catalogName
    schema_name := input.action.resource.table.schemaName
    table_name := input.action.resource.table.tableName
    some policy in user_policies
    field_matches(policy.catalog, catalog_name)
    field_matches(policy.schema_name, schema_name)
    field_matches(policy.table_name, table_name)
    has_privilege(policy, "UPDATE_TABLE")
}
