# Table-level authorization rules

package trino

import rego.v1

import data.rbac
import data.trino

# ============================================================================
# TABLE-LEVEL OPERATIONS
# ============================================================================

# ShowTables - user needs SHOW_TABLES privilege
allow_table if {
    input.action.operation == "ShowTables"
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    rbac.check_permission(trino.user_id, resource, "ShowTables")
}

# CreateTable - user needs CREATE_TABLE privilege
allow_table if {
    input.action.operation == "CreateTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "CreateTable")
}

# DropTable - user needs DROP_TABLE privilege
allow_table if {
    input.action.operation == "DropTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "DropTable")
}

# RenameTable - user needs DROP_TABLE privilege
allow_table if {
    input.action.operation == "RenameTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "RenameTable")
}

# SetTableComment - user needs UPDATE_TABLE privilege
allow_table if {
    input.action.operation == "SetTableComment"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "SetTableComment")
}

# SetTableAuthorization - user needs DROP_TABLE privilege
allow_table if {
    input.action.operation == "SetTableAuthorization"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "SetTableAuthorization")
}

# ============================================================================
# DATA OPERATIONS
# ============================================================================

# SelectFromColumns - user needs SELECT_TABLE privilege
allow_table if {
    input.action.operation == "SelectFromColumns"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "SelectFromColumns")
}

# InsertIntoTable - user needs INSERT_TABLE privilege
allow_table if {
    input.action.operation == "InsertIntoTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "InsertIntoTable")
}

# DeleteFromTable - user needs DELETE_TABLE privilege
allow_table if {
    input.action.operation == "DeleteFromTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "DeleteFromTable")
}

# UpdateTableColumns - user needs UPDATE_TABLE privilege
allow_table if {
    input.action.operation == "UpdateTableColumns"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "UpdateTableColumns")
}

# TruncateTable - user needs DELETE_TABLE privilege
allow_table if {
    input.action.operation == "TruncateTable"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "TruncateTable")
}

# ============================================================================
# COLUMN OPERATIONS
# ============================================================================

# ShowColumns - user needs SHOW_COLUMNS privilege
allow_table if {
    input.action.operation == "ShowColumns"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "ShowColumns")
}

# FilterColumns - filter columns based on user permissions
allow_table if {
    input.action.operation == "FilterColumns"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "FilterColumns")
}

# FilterTables - filter tables based on user permissions
allow_table if {
    input.action.operation == "FilterTables"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "FilterTables")
}

# AddColumn, DropColumn, RenameColumn, SetColumnComment - UPDATE_TABLE
allow_table if {
    input.action.operation in ["AddColumn", "DropColumn", "RenameColumn", "SetColumnComment"]
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, input.action.operation)
}

# ============================================================================
# VIEW OPERATIONS
# ============================================================================

# CreateView, CreateMaterializedView - CREATE_TABLE privilege
allow_table if {
    input.action.operation in ["CreateView", "CreateMaterializedView"]
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, input.action.operation)
}

# DropView, DropMaterializedView - DROP_TABLE privilege
allow_table if {
    input.action.operation in ["DropView", "DropMaterializedView"]
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, input.action.operation)
}

# RenameView - DROP_TABLE privilege
allow_table if {
    input.action.operation == "RenameView"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "RenameView")
}

# SetViewComment - UPDATE_TABLE privilege
allow_table if {
    input.action.operation == "SetViewComment"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "SetViewComment")
}

# RefreshMaterializedView - UPDATE_TABLE privilege
allow_table if {
    input.action.operation == "RefreshMaterializedView"
    resource := trino.build_resource(input.action.resource)
    rbac.check_permission(trino.user_id, resource, "RefreshMaterializedView")
}

