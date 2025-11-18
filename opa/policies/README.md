# OPA Policy for RBAC API - Modular Structure

## Overview

This directory contains the modularized OPA policy for Trino authorization using RBAC API. The policy is organized into logical modules for better maintainability and clarity.

## Directory Structure

```
policy-opa/
├── configuration.rego          # RBAC API configuration (URL, timeout)
├── rbac/
│   ├── authentication.rego     # HTTP communication with RBAC API
│   └── check.rego             # Permission check functions
└── trino/
    ├── main.rego              # Entry point (aggregates all rules)
    ├── user.rego              # User ID extraction
    ├── helpers.rego           # Resource extraction helpers
    ├── allow_catalog.rego     # Catalog-level authorization
    ├── allow_schema.rego      # Schema-level authorization
    ├── allow_table.rego       # Table-level authorization
    ├── allow_system.rego      # System catalog special rules
    └── batch.rego             # Batch operations and column masking
```

## Module Descriptions

### configuration.rego

- **Package**: `configuration`
- **Purpose**: Global configuration for RBAC API integration
- **Key Variables**:
  - `rbac_api_url`: RBAC API endpoint (default: `http://rbac-api:8000`)
  - `rbac_api_timeout`: HTTP timeout (default: `2s`)
  - `debug_enabled`: Enable debug logging (default: `false`)

### rbac/authentication.rego

- **Package**: `rbac`
- **Purpose**: HTTP communication layer with RBAC API
- **Key Functions**:
  - `call_permission_check(user_id, resource, operation)`: Makes HTTP POST to `/permissions/check`

### rbac/check.rego

- **Package**: `rbac`
- **Purpose**: Permission checking logic
- **Key Functions**:
  - `check_permission(user_id, resource, operation)`: Returns boolean (allowed/denied)

### trino/user.rego

- **Package**: `trino`
- **Purpose**: User identity extraction
- **Key Variables**:
  - `user_id`: Extracted from `input.context.identity.user`

### trino/helpers.rego

- **Package**: `trino`
- **Purpose**: Resource extraction and transformation utilities
- **Key Functions**:
  - `get_catalog_name(input_resource)`: Extract catalog name
  - `get_schema_name(input_resource)`: Extract schema name
  - `get_table_name(input_resource)`: Extract table name
  - `build_resource(input_resource)`: Build resource object for RBAC API

### trino/allow_catalog.rego

- **Package**: `trino`
- **Purpose**: Catalog-level authorization rules
- **Operations Handled**:
  - `AccessCatalog`, `CreateCatalog`, `DropCatalog`
  - `FilterCatalogs`, `ShowCatalogs`

### trino/allow_schema.rego

- **Package**: `trino`
- **Purpose**: Schema-level authorization rules
- **Operations Handled**:
  - `ShowSchemas`, `CreateSchema`, `DropSchema`
  - `RenameSchema`, `SetSchemaAuthorization`, `FilterSchemas`

### trino/allow_table.rego

- **Package**: `trino`
- **Purpose**: Table-level authorization rules
- **Operations Handled**:
  - **Table ops**: `ShowTables`, `CreateTable`, `DropTable`, `RenameTable`
  - **Data ops**: `SelectFromColumns`, `InsertIntoTable`, `DeleteFromTable`, `UpdateTableColumns`
  - **Column ops**: `ShowColumns`, `FilterColumns`, `AddColumn`, `DropColumn`
  - **View ops**: `CreateView`, `DropView`, `RefreshMaterializedView`

### trino/allow_system.rego

- **Package**: `trino`
- **Purpose**: Special rules for system catalog and information_schema
- **Special Cases**:
  - System catalog access (always allowed for authenticated users)
  - information_schema access (allowed if user has catalog access)
  - `ExecuteQuery` (allowed for any user with permissions)

### trino/batch.rego

- **Package**: `trino`
- **Purpose**: Batch operations and column masking
- **Key Rules**:
  - `batch`: Set for filtering resources (`FilterResources`)
  - Special handling for `FilterColumns` operation
  - `batchColumnMasks`: Column masking (placeholder for future)

### trino/main.rego

- **Package**: `trino`
- **Purpose**: Main entry point that aggregates all authorization rules
- **Default**: `allow := false`
- **Aggregates**:
  - `allow_catalog`, `allow_schema`, `allow_table`, `allow_system`

## Usage

### Loading the Policy

There are two ways to load this policy in OPA:

#### Option 1: Load the entire directory (Recommended)

```bash
opa run --server --bundle policy-opa/
```

This will load all modules and make `trino.allow` available as the decision point.

#### Option 2: Specify main entry point

Configure your Trino OPA integration to query: `data.trino.allow`

### Environment Variables

Configure the following environment variables:

```bash
# RBAC API URL (default: http://rbac-api:8000)
RBAC_API_URL=http://rbac-api:8000

# HTTP timeout for RBAC API calls (default: 2s)
RBAC_API_TIMEOUT=2s

# Enable debug logging (default: false)
RBAC_DEBUG=false
```

### Docker Compose Example

```yaml
opa:
  image: openpolicyagent/opa:latest
  command:
    - "run"
    - "--server"
    - "--bundle"
    - "/policy-opa"
  volumes:
    - ./policy-opa:/policy-opa
  environment:
    - RBAC_API_URL=http://rbac-api:8000
    - RBAC_API_TIMEOUT=2s
  ports:
    - "8181:8181"
```

### Trino Configuration

In Trino's `access-control.properties`:

```properties
access-control.name=opa
opa.uri=http://opa:8181/v1/data/trino/allow
opa.batch-uri=http://opa:8181/v1/data/trino/batch
```

## Testing

### Test Individual Modules

```bash
# Test configuration
opa eval -d policy-opa/ -i test-input.json "data.configuration.rbac_api_url"

# Test permission check
opa eval -d policy-opa/ -i test-input.json "data.rbac.check_permission(\"alice\", {\"catalog_name\": \"prod\"}, \"AccessCatalog\")"

# Test resource extraction
opa eval -d policy-opa/ -i test-input.json "data.trino.build_resource(input.action.resource)"
```

### Test Authorization Decision

```bash
# Test full authorization
opa eval -d policy-opa/ -i test-input.json "data.trino.allow"
```

Example test input (`test-input.json`):

```json
{
  "context": {
    "identity": {
      "user": "alice"
    }
  },
  "action": {
    "operation": "SelectFromColumns",
    "resource": {
      "table": {
        "catalogName": "prod",
        "schemaName": "default",
        "tableName": "users"
      }
    }
  }
}
```

## Module Dependencies

```
configuration.rego
       ↓
rbac/authentication.rego → uses configuration
       ↓
rbac/check.rego → uses authentication
       ↓
trino/user.rego, trino/helpers.rego
       ↓
trino/allow_*.rego → use rbac.check_permission
       ↓
trino/main.rego → aggregates all rules
```

## Migration from Monolithic authz.rego

The original `authz.rego` has been refactored into this modular structure:

| Original Location | New Location               |
| ----------------- | -------------------------- |
| HTTP calls        | `rbac/authentication.rego` |
| Permission check  | `rbac/check.rego`          |
| User extraction   | `trino/user.rego`          |
| Resource helpers  | `trino/helpers.rego`       |
| Catalog rules     | `trino/allow_catalog.rego` |
| Schema rules      | `trino/allow_schema.rego`  |
| Table rules       | `trino/allow_table.rego`   |
| System rules      | `trino/allow_system.rego`  |
| Batch ops         | `trino/batch.rego`         |
| Main entry        | `trino/main.rego`          |

## Benefits of Modular Structure

1. **Separation of Concerns**: Each module has a clear, single responsibility
2. **Maintainability**: Easier to update specific authorization rules
3. **Readability**: Smaller files are easier to understand
4. **Testability**: Individual modules can be tested in isolation
5. **Reusability**: Helper functions are centralized
6. **Scalability**: Easy to add new authorization rules or resource types

## Future Enhancements

- Column-level masking implementation in `batch.rego`
- Additional resource types (functions, procedures)
- Role-based permissions
- Time-based access control
- Audit logging integration
