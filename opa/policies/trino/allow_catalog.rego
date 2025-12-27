# Catalog-level authorization rules
#
# Note: The Permission API automatically resolves catalog_name to warehouse name
# (using mapping if catalog name differs from warehouse name) and checks warehouse permissions.
# So if a user has permission on a warehouse, they automatically have permission
# on the corresponding catalog (e.g., catalog "lakekeeper" -> warehouse "bronze").

package trino

import rego.v1

import data.rbac
import data.trino

# ============================================================================
# CATALOG-LEVEL OPERATIONS
# ============================================================================

# AccessCatalog - user needs ACCESS_CATALOG privilege
# The Permission API will check warehouse permission (catalog name = warehouse name)
allow_catalog if {
    input.action.operation == "AccessCatalog"
    catalog_name := trino.get_catalog_name(input.action.resource)
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "AccessCatalog")
}

# CreateCatalog - user needs CREATE_CATALOG privilege
# The Permission API will check warehouse permission
allow_catalog if {
    input.action.operation == "CreateCatalog"
    catalog_name := trino.get_catalog_name(input.action.resource)
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "CreateCatalog")
}

# DropCatalog - user needs DROP_CATALOG privilege
# The Permission API will check warehouse permission
allow_catalog if {
    input.action.operation == "DropCatalog"
    catalog_name := trino.get_catalog_name(input.action.resource)
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "DropCatalog")
}

# FilterCatalogs - filter catalogs based on user permissions
# The Permission API will check warehouse permission
allow_catalog if {
    input.action.operation == "FilterCatalogs"
    catalog_name := trino.get_catalog_name(input.action.resource)
    resource := {"catalog_name": catalog_name}
    rbac.check_permission(trino.user_id, resource, "FilterCatalogs")
}

# ShowCatalogs - check if user has any permissions
allow_catalog if {
    input.action.operation == "ShowCatalogs"
    rbac.check_permission(trino.user_id, {}, "ShowCatalogs")
}

