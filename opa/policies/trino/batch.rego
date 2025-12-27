# Batch operations and column masking

package trino

import rego.v1

import data.trino

# ============================================================================
# BATCH OPERATIONS (FilterResources)
# ============================================================================

# Batch filtering for resources
# For each resource in filterResources, check if it's allowed
batch contains i if {
    some i
    raw_resource := input.action.filterResources[i]
    allow with input.action.resource as raw_resource
}

# Corner case: filtering columns
# When filtering columns, Trino sends a single table with multiple columns
# We need to check each column individually
batch contains i if {
    some i
    input.action.operation == "FilterColumns"
    count(input.action.filterResources) == 1
    raw_resource := input.action.filterResources[0]
    count(raw_resource["table"]["columns"]) > 0
    new_resources := [
        object.union(raw_resource, {"table": {"column": column_name}})
        | column_name := raw_resource["table"]["columns"][_]
    ]
    allow with input.action.resource as new_resources[i]
}

# ============================================================================
# COLUMN MASKING
# ============================================================================

# batchColumnMasks is defined in column_mask.rego

