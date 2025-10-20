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
            'ALL'
        );
    END IF;
END$$;

