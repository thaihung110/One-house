-- Initialize RBAC database
-- This script runs automatically when the PostgreSQL container starts for the first time

-- 1) Create object_type enum for relation table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'object_type_enum') THEN
        CREATE TYPE object_type_enum AS ENUM (
            'catalog',
            'schema',
            'table'
        );
    END IF;
END$$;

-- 2) Create privilege_enum type (idempotent). If exists, add missing values.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'privilege_enum') THEN
        CREATE TYPE privilege_enum AS ENUM (
            'SELECT_TABLE',
            'INSERT_TABLE',
            'UPDATE_TABLE',
            'DELETE_TABLE',
            'CREATE_TABLE',
            'DROP_TABLE',
            'SHOW_TABLES',
            'CREATE_SCHEMA',
            'DROP_SCHEMA',
            'ALTER_SCHEMA',
            'SHOW_SCHEMAS',
            'CREATE_CATALOG',
            'DROP_CATALOG',
            'SHOW_CATALOGS',
            'SHOW_COLUMNS',
            'ACCESS_CATALOG',
            'ACCESS_SCHEMA',
            'ALL'
        );
    ELSE
        -- Ensure all expected values exist (PostgreSQL 13+ supports IF NOT EXISTS)
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'SELECT_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'INSERT_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'UPDATE_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'DELETE_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'CREATE_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'DROP_TABLE';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'SHOW_TABLES';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'CREATE_SCHEMA';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'DROP_SCHEMA';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'ALTER_SCHEMA';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'SHOW_SCHEMAS';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'CREATE_CATALOG';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'DROP_CATALOG';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'SHOW_CATALOGS';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'SHOW_COLUMNS';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'ACCESS_CATALOG';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'ACCESS_SCHEMA';
        ALTER TYPE privilege_enum ADD VALUE IF NOT EXISTS 'ALL';
    END IF;
END$$;

-- 3) Create catalog table
CREATE TABLE IF NOT EXISTS catalog (
    catalog_id SERIAL PRIMARY KEY,
    catalog_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4) Create schema table
CREATE TABLE IF NOT EXISTS schema (
    schema_id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    catalog_id INTEGER NOT NULL REFERENCES catalog(catalog_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_schema_name_catalog UNIQUE (schema_name, catalog_id)
);

-- 5) Create table table
CREATE TABLE IF NOT EXISTS "table" (
    table_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id) ON DELETE CASCADE,
    catalog_id INTEGER NOT NULL REFERENCES catalog(catalog_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_table_name_schema UNIQUE (table_name, schema_id)
);

-- 6) Create relation table for permissions
CREATE TABLE IF NOT EXISTS relation (
    relation_id SERIAL PRIMARY KEY,
    object_id INTEGER NOT NULL,
    object_type object_type_enum NOT NULL,
    user_id TEXT NOT NULL,
    actions privilege_enum[] NOT NULL DEFAULT ARRAY[]::privilege_enum[],
    columns TEXT[],  -- Only applicable for table-level permissions
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_actions_not_empty CHECK (array_length(actions, 1) > 0),
    CONSTRAINT uq_relation_user_object UNIQUE (user_id, object_id, object_type, columns)
);

-- 7) Indexes for performance

-- Catalog indexes
CREATE INDEX IF NOT EXISTS idx_catalog_name ON catalog(catalog_name);

-- Schema indexes
CREATE INDEX IF NOT EXISTS idx_schema_catalog ON schema(catalog_id);
CREATE INDEX IF NOT EXISTS idx_schema_name ON schema(schema_name);

-- Table indexes
CREATE INDEX IF NOT EXISTS idx_table_schema ON "table"(schema_id);
CREATE INDEX IF NOT EXISTS idx_table_catalog ON "table"(catalog_id);
CREATE INDEX IF NOT EXISTS idx_table_name ON "table"(table_name);

-- Relation indexes for fast permission lookup
CREATE INDEX IF NOT EXISTS idx_relation_user ON relation(user_id);
CREATE INDEX IF NOT EXISTS idx_relation_user_type ON relation(user_id, object_type);
CREATE INDEX IF NOT EXISTS idx_relation_object ON relation(object_id, object_type);

-- 8) Create update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 9) Apply triggers to all tables
DO $$
BEGIN
    -- Catalog trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_catalog_updated_at') THEN
        CREATE TRIGGER update_catalog_updated_at
            BEFORE UPDATE ON catalog
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;

    -- Schema trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_schema_updated_at') THEN
        CREATE TRIGGER update_schema_updated_at
            BEFORE UPDATE ON schema
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;

    -- Table trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_table_updated_at') THEN
        CREATE TRIGGER update_table_updated_at
            BEFORE UPDATE ON "table"
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;

    -- Relation trigger
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_relation_updated_at') THEN
        CREATE TRIGGER update_relation_updated_at
            BEFORE UPDATE ON relation
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END$$;
