-- Database initialization script for consolidated PostgreSQL instance
-- Creates separate databases and dedicated users for Lakekeeper, OpenFGA, and Keycloak

-- Create users for each service
CREATE USER lakekeeper_admin WITH PASSWORD 'lakekeeper_password';
CREATE USER openfga_admin WITH PASSWORD 'openfga_password';
CREATE USER keycloak_admin WITH PASSWORD 'keycloak_password';
CREATE USER airflow_admin WITH PASSWORD 'airflow_password';

-- Create databases
CREATE DATABASE lakekeeper_db OWNER lakekeeper_admin;
CREATE DATABASE openfga_db OWNER openfga_admin;
CREATE DATABASE keycloak_db OWNER keycloak_admin;
CREATE DATABASE airflow_db OWNER airflow_admin;

-- Grant all privileges to respective users on their databases
GRANT ALL PRIVILEGES ON DATABASE lakekeeper_db TO lakekeeper_admin;
GRANT ALL PRIVILEGES ON DATABASE openfga_db TO openfga_admin;
GRANT ALL PRIVILEGES ON DATABASE keycloak_db TO keycloak_admin;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_admin;

-- Grant connection privileges
GRANT CONNECT ON DATABASE lakekeeper_db TO lakekeeper_admin;
GRANT CONNECT ON DATABASE openfga_db TO openfga_admin;
GRANT CONNECT ON DATABASE keycloak_db TO keycloak_admin;
GRANT CONNECT ON DATABASE airflow_db TO airflow_admin;

-- Display created databases and users
\l
\du
