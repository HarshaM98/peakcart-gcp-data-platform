-- Run once against the legacy Postgres instance (as the postgres admin
-- user) after cloudsql.logical_decoding=on has been applied and the
-- instance has restarted. Creates the publication and replication slot
-- Datastream's stream config references by name.

CREATE PUBLICATION datastream_publication FOR ALL TABLES;

SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');
