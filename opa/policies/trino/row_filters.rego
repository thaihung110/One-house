# Row filtering policy for Trino
# Returns row filters as array of objects with "expression" field
# Trino calls this endpoint: /v1/data/trino/rowFilters

package trino

import rego.v1

import data.configuration
import data.trino

# ============================================================================
# ROW FILTERS
# ============================================================================

# Get row filter from Permission API
# Returns SQL WHERE clause or null
get_row_filter() := filter if {
    # Only for SelectFromColumns operation
    input.action.operation == "SelectFromColumns"
    
    # Build table FQN
    catalog_name := trino.get_catalog_name(input.action.resource)
    schema_name := trino.get_schema_name(input.action.resource)
    table_name := trino.get_table_name(input.action.resource)
    
    # Call Permission API
    response := http.send({
        "method": "POST",
        "url": sprintf("%s/permissions/row-filter", [configuration.permission_api_url]),
        "headers": {
            "Content-Type": "application/json"
        },
        "body": {
            "user_id": trino.user_id,
            "resource": {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name
            }
        },
        "timeout": configuration.permission_api_timeout
    })
    
    # Permission API returns: {"filter_expression": "...", "has_filter": true}
    # Check if response is successful and has filter
    response.status_code == 200
    response.body.has_filter == true
    filter := response.body.filter_expression
    filter != null
}

# Return row filter in Trino's expected format
# Trino expects: array of objects with "expression" field
# Format: [{"expression": "clause"}]
# Each expression behaves like an additional WHERE clause
rowFilters contains {"expression": filter} if {
    filter := get_row_filter()
}

