CREATE EXTENSION IF NOT EXISTS "postgres_fdw";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP SERVER IF EXISTS pnf_svr CASCADE;
CREATE SERVER pnf_svr
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'XX.XX.XX.XX', dbname 'DB_NAME', port '5432');

CREATE SCHEMA IF NOT EXISTS programme_externe;

CREATE USER MAPPING FOR postgres
SERVER pnf_svr
OPTIONS (user 'USER', password 'PASSWORD');