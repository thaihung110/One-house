-- Initialize RBAC database
-- This script runs automatically when the PostgreSQL container starts for the first time

-- Create the privilege_enum type if it doesn't exist
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
            'CREATE_SCHEMA',
            'DROP_SCHEMA',
            'CREATE_CATALOG',
            'DROP_CATALOG',
            'ACCESS_CATALOG',
            'ALL'
        );
    END IF;
END$$;

CREATE TABLE policy (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    catalog TEXT,
    schema_name TEXT,
    table_name TEXT,
    columns TEXT[],
    actions privilege_enum[]
);

INSERT INTO policy (username, catalog, schema_name, table_name, columns, actions)
VALUES ('hung', NULL, NULL, NULL, NULL, ARRAY['ALL']::privilege_enum[]);


