# Batch column masking policy for Trino (GetColumnMask)
#
# Default: unmask. Only mask when a MaskColumn tuple exists in OpenFGA
# (checked via permission-api). If no mask tuple, we return no entry and
# Trino keeps the original value.

package trino

import rego.v1
import future.keywords.in
import future.keywords.if
import future.keywords.contains

import data.rbac

# Only handle GetColumnMask (batch)
batchColumnMasks := masks if {
    input.action.operation == "GetColumnMask"
    masks := [mask_entry(i) |
        some i
        input.action.filterResources[i]
        col := input.action.filterResources[i].column
        needs_mask(col)
    ]
}

# Build a mask entry for column index i
mask_entry(i) := {
    "index": i,
    "viewExpression": {
        "expression": "substring(phone_number, 1, 3) || '******'",  # show first 3 digits + mask rest
    },
}

# Mask-by-exception: default unmask; only mask when a `mask` tuple exists
needs_mask(col) if {
    resp := rbac.call_permission_check(
        input.context.identity.user,
        {
            "catalog": col.catalogName,
            "schema":  col.schemaName,
            "table":   col.tableName,
            "column":  col.columnName,
        },
        "MaskColumn",  # mapped in permission-api to relation `mask` on column object
    )
    resp.status_code == 200
    resp.body.allowed == true
}

