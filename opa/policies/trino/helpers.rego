#! Helper functions for resource extraction and building
package trino

import rego.v1

# ============================================================================
# RESOURCE EXTRACTION HELPERS
# ============================================================================

# Extract catalog name from input resource
# Handles different input shapes (catalog, schema, table)
get_catalog_name(input_resource) := catalog_name if {
    catalog_name := input_resource.catalog.name
}

get_catalog_name(input_resource) := catalog_name if {
    catalog_name := input_resource.catalog.catalogName
}

get_catalog_name(input_resource) := catalog_name if {
    catalog_name := input_resource.catalogName
}

get_catalog_name(input_resource) := catalog_name if {
    catalog_name := input_resource.schema.catalogName
}

get_catalog_name(input_resource) := catalog_name if {
    catalog_name := input_resource.table.catalogName
}

# Extract schema name from input resource
get_schema_name(input_resource) := schema_name if {
    schema_name := input_resource.schema.schemaName
}

get_schema_name(input_resource) := schema_name if {
    schema_name := input_resource.table.schemaName
}

get_schema_name(input_resource) := schema_name if {
    schema_name := input_resource.schemaName
}

# Extract table name from input resource
get_table_name(input_resource) := table_name if {
    table_name := input_resource.table.tableName
}

get_table_name(input_resource) := table_name if {
    table_name := input_resource.tableName
}

# Extract columns from input resource
get_columns(input_resource) := columns if {
    columns := input_resource.table.columns
}

get_columns(input_resource) := columns if {
    column := input_resource.table.column
    columns := [column]
}

get_columns(input_resource) := columns if {
    columns := input_resource.columns
}

# Predicate: does the resource reference any columns?
has_columns(input_resource) if {
    _ := input_resource.table.columns
}

has_columns(input_resource) if {
    _ := input_resource.table.column
}

has_columns(input_resource) if {
    _ := input_resource.columns
}

# Predicate: does the resource include a table?
has_table(input_resource) if {
    _ := input_resource.table
}

has_table(input_resource) if {
    _ := input_resource.tableName
}

# Predicate: does the resource include a schema?
has_schema(input_resource) if {
    _ := input_resource.schema
}

has_schema(input_resource) if {
    _ := input_resource.schemaName
}

has_schema(input_resource) if {
    has_table(input_resource)
    _ := input_resource.table.schemaName
}

# ============================================================================
# RESOURCE BUILDER FOR RBAC API
# ============================================================================

# Build resource object for RBAC API - table level (full hierarchy)
build_resource(input_resource) := resource if {
    has_table(input_resource)
    catalog_name := get_catalog_name(input_resource)
    schema_name := get_schema_name(input_resource)
    table_name := get_table_name(input_resource)
    base := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }
    resource := maybe_add_columns(base, input_resource)
}

# Build resource object for RBAC API - schema level
build_resource(input_resource) := resource if {
    not has_table(input_resource)
    has_schema(input_resource)
    catalog_name := get_catalog_name(input_resource)
    schema_name := get_schema_name(input_resource)
    resource := {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
}

# Build resource object for RBAC API - catalog level
build_resource(input_resource) := resource if {
    not has_table(input_resource)
    not has_schema(input_resource)
    catalog_name := get_catalog_name(input_resource)
    resource := {"catalog_name": catalog_name}
}

# Helper to optionally merge columns into the resource map
maybe_add_columns(base, input_resource) := object.union(base, {"columns": columns}) if {
    has_columns(input_resource)
    columns := get_columns(input_resource)
}

maybe_add_columns(base, input_resource) := base if {
    not has_columns(input_resource)
}

# ============================================================================
# SYSTEM CATALOG HELPERS
# ============================================================================

# Predicate: is this operation targeting the Trino system catalog?
#
# Many Trino internal metadata queries use catalog `system` (e.g. system.jdbc.*).
# We don't want to enforce RBAC on these technical/system objects, so OPA
# can allow all operations when the catalog is `system`.
is_system_catalog_operation if {
    catalog_name := get_catalog_name(input.action.resource)
    catalog_name == "system"
}

