#!/bin/bash
# Creates multiple PostgreSQL databases on startup
set -e

function create_user_and_database() {
    local database=$1
    local user=$2
    local password=$3
    echo "Creating user '$user' and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $user WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL
}

create_user_and_database nextgen_databridge_audit nextgen_databridge nextgen_databridge
create_user_and_database nextgen_databridge_config nextgen_databridge nextgen_databridge

# Grant cross-db access for audit user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    GRANT ALL PRIVILEGES ON DATABASE airflow TO nextgen_databridge;
EOSQL

echo "Multiple databases created successfully"
