#! Main entry point for Trino authorization
#! Aggregates all authorization rules

package trino

import rego.v1

import data.trino

# ============================================================================
# MAIN AUTHORIZATION DECISION
# ============================================================================

# Default deny
default allow := false

# Allow all operations on Trino system catalog (e.g. system.jdbc.*)
# These are internal metadata queries and should not be blocked by RBAC.
allow if {
    trino.is_system_catalog_operation
}

# Allow if any catalog-level rule matches
allow if {
    trino.allow_catalog
}

# Allow if any schema-level rule matches
allow if {
    trino.allow_schema
}

# Allow if any table-level rule matches
allow if {
    trino.allow_table
}

# Allow if any system-level rule matches
allow if {
    trino.allow_system
}

