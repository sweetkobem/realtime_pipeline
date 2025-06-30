CREATE ROLE debezium_user WITH LOGIN PASSWORD '{replace with your password}' REPLICATION;

-- Give permission to all table 
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Create publication
CREATE PUBLICATION dbz_publication FOR ALL TABLES;