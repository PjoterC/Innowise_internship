-- Layer 0 — one database, one schema per storage layer.
--
--   RAW  (stage 1) landing zone: 1:1 with the source file, all VARCHAR, append-only.
--   CORE (stage 2) typed star schema: 3 dimensions + 1 fact, loaded by MERGE.
--   MART (stage 3) consumer layer: aggregate table + secure view.
--   META           pipeline bookkeeping: audit log and row-level-security map.
--
-- Time Travel retention, inherited by every schema and table below. Enterprise
-- Edition allows up to 90 days (Standard caps at 1); 7 is enough to recover
-- from a bad load discovered the following Monday, without paying to keep three
-- months of every intermediate version of the landing table.

CREATE DATABASE IF NOT EXISTS AIRLINE_DWH
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Airline booking DWH: RAW -> CORE -> MART, orchestrated by Airflow.';

USE DATABASE AIRLINE_DWH;

CREATE SCHEMA IF NOT EXISTS RAW  COMMENT = 'Stage 1 — landing zone.';
CREATE SCHEMA IF NOT EXISTS CORE COMMENT = 'Stage 2 — star schema.';
CREATE SCHEMA IF NOT EXISTS MART COMMENT = 'Stage 3 — consumer layer.';
CREATE SCHEMA IF NOT EXISTS META COMMENT = 'Audit log and RLS map.';

DROP SCHEMA IF EXISTS AIRLINE_DWH.PUBLIC;
