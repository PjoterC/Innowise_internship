-- Two DDL statements that use Time Travel.
--
-- The retention window is the 7 days DATA_RETENTION_TIME_IN_DAYS sets in
-- 00_database_and_schemas.sql (Enterprise allows up to 90). Everything below
-- has to fall inside it, and inside the table's own lifetime.

USE DATABASE AIRLINE_DWH;

-- --- DDL 1: zero-copy clone of the fact as it was five minutes ago ----------

CREATE OR REPLACE TABLE CORE.FCT_FLIGHT_BOOKING_5MIN_AGO
    CLONE CORE.FCT_FLIGHT_BOOKING
    AT (OFFSET => -300);

SELECT (SELECT COUNT(*) FROM CORE.FCT_FLIGHT_BOOKING)          AS rows_now,
       (SELECT COUNT(*) FROM CORE.FCT_FLIGHT_BOOKING_5MIN_AGO) AS rows_5min_ago;

-- --- DDL 2: recover a dropped table -----------------------------------------
-- UNDROP is Time Travel too: the dropped table stays retrievable for the retention window rather than being deleted.
DROP TABLE MART.AGG_FLIGHT_STATUS_DAILY;

SHOW TABLES HISTORY LIKE 'AGG_FLIGHT_STATUS_DAILY' IN SCHEMA MART;  -- dropped_on is set

UNDROP TABLE MART.AGG_FLIGHT_STATUS_DAILY;

SELECT COUNT(*) AS rows_after_undrop FROM MART.AGG_FLIGHT_STATUS_DAILY;

-- Clean up the clone; the original is untouched.
DROP TABLE IF EXISTS CORE.FCT_FLIGHT_BOOKING_5MIN_AGO;
