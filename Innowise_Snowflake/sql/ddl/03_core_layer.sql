-- CORE — stage 2. A star: three dimensions around one fact at the grain of
-- "one booked seat".
--
-- Surrogate keys are MD5 hashes of the natural key rather than sequences, so
-- both sides can compute the same key independently and the fact never has to
-- look a dimension up. DIM_DATE keeps the classic integer YYYYMMDD key, which
-- gives the "unknown" member an obvious spelling (-1).
--
-- RECORD_HASH is what makes SCD type 1 honest: the MERGE only updates when the
-- hash differs, so re-running over unchanged data audits as 0 updated instead
-- of 98,619 no-op writes.
--
-- Profiled before modelling: "Pilot Name" equals the passenger's own name in
-- 100% of rows (source junk, dropped); "Arrival Airport" is the IATA code of
-- the airport in "Airport Name" and neither is unique alone, so the airport's
-- natural key is code + name + country; "Ticket Type" and "Passenger Status"
-- are constant across all rows (kept for fidelity, useless for analysis).
--
-- Constraints are declared but not enforced — Snowflake only enforces NOT NULL.
-- They are the model's documentation, and BI tools read them to infer joins.

USE DATABASE AIRLINE_DWH;

CREATE TABLE IF NOT EXISTS CORE.DIM_PASSENGER (
    PASSENGER_KEY   VARCHAR(32)   NOT NULL,   -- MD5(PASSENGER_ID)
    PASSENGER_ID    VARCHAR       NOT NULL,
    FIRST_NAME      VARCHAR,
    LAST_NAME       VARCHAR,
    GENDER          VARCHAR,
    AGE             NUMBER(3,0),
    NATIONALITY     VARCHAR,
    RECORD_HASH     VARCHAR(32)   NOT NULL,
    DWH_INSERTED_AT TIMESTAMP_LTZ NOT NULL,
    DWH_UPDATED_AT  TIMESTAMP_LTZ NOT NULL,
    CONSTRAINT PK_DIM_PASSENGER PRIMARY KEY (PASSENGER_KEY)
);

-- CONTINENT_CODE is the axis row-level security filters on.
CREATE TABLE IF NOT EXISTS CORE.DIM_AIRPORT (
    AIRPORT_KEY     VARCHAR(32)   NOT NULL,   -- MD5(code|name|country_code)
    AIRPORT_CODE    VARCHAR       NOT NULL,
    AIRPORT_NAME    VARCHAR       NOT NULL,
    COUNTRY_CODE    VARCHAR,
    COUNTRY_NAME    VARCHAR,
    CONTINENT_CODE  VARCHAR,                  -- AF AS EU NAM OC SAM
    CONTINENT_NAME  VARCHAR,
    RECORD_HASH     VARCHAR(32)   NOT NULL,
    DWH_INSERTED_AT TIMESTAMP_LTZ NOT NULL,
    DWH_UPDATED_AT  TIMESTAMP_LTZ NOT NULL,
    CONSTRAINT PK_DIM_AIRPORT PRIMARY KEY (AIRPORT_KEY)
);

-- Densified from the dates actually present in RAW rather than pre-generated
-- for a 50-year range: the source covers one year.
CREATE TABLE IF NOT EXISTS CORE.DIM_DATE (
    DATE_KEY        NUMBER(8,0)   NOT NULL,   -- YYYYMMDD, -1 = unknown
    FULL_DATE       DATE,
    DAY_NAME        VARCHAR,
    IS_WEEKEND      BOOLEAN,
    MONTH_NUMBER    NUMBER(2,0),
    MONTH_NAME      VARCHAR,
    QUARTER_NUMBER  NUMBER(1,0),
    YEAR_NUMBER     NUMBER(4,0),
    DWH_INSERTED_AT TIMESTAMP_LTZ NOT NULL,
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
);

-- The unknown member: a fact row whose date will not parse points here rather
-- than carrying a NULL, so MART's joins stay INNER and a broken date shows up
-- as a visible bucket instead of a silently missing row.
MERGE INTO CORE.DIM_DATE t
USING (SELECT -1 AS DATE_KEY) s ON t.DATE_KEY = s.DATE_KEY
WHEN NOT MATCHED THEN INSERT (DATE_KEY, DAY_NAME, MONTH_NAME, DWH_INSERTED_AT)
                      VALUES (-1, 'Unknown', 'Unknown', CURRENT_TIMESTAMP());

-- Natural key is the source file's own row index: stable across reloads of the
-- same file, which is what makes the MERGE idempotent. The IS_* flags are
-- additive so MART can SUM them; FLIGHT_STATUS is kept because the string is
-- what a human wants to read.
CREATE TABLE IF NOT EXISTS CORE.FCT_FLIGHT_BOOKING (
    BOOKING_KEY        VARCHAR(32)   NOT NULL,   -- MD5(BOOKING_ID)
    BOOKING_ID         NUMBER(38,0)  NOT NULL,   -- source row index
    PASSENGER_KEY      VARCHAR(32)   NOT NULL,
    AIRPORT_KEY        VARCHAR(32)   NOT NULL,
    DEPARTURE_DATE_KEY NUMBER(8,0)   NOT NULL,
    FLIGHT_STATUS      VARCHAR,
    TICKET_TYPE        VARCHAR,
    PASSENGER_STATUS   VARCHAR,
    IS_ON_TIME         NUMBER(1,0)   NOT NULL DEFAULT 0,
    IS_DELAYED         NUMBER(1,0)   NOT NULL DEFAULT 0,
    IS_CANCELLED       NUMBER(1,0)   NOT NULL DEFAULT 0,
    RECORD_HASH        VARCHAR(32)   NOT NULL,
    DWH_INSERTED_AT    TIMESTAMP_LTZ NOT NULL,
    DWH_UPDATED_AT     TIMESTAMP_LTZ NOT NULL,
    CONSTRAINT PK_FCT_FLIGHT_BOOKING PRIMARY KEY (BOOKING_KEY),
    CONSTRAINT FK_FCT_PASSENGER FOREIGN KEY (PASSENGER_KEY)      REFERENCES CORE.DIM_PASSENGER (PASSENGER_KEY),
    CONSTRAINT FK_FCT_AIRPORT   FOREIGN KEY (AIRPORT_KEY)        REFERENCES CORE.DIM_AIRPORT   (AIRPORT_KEY),
    CONSTRAINT FK_FCT_DATE      FOREIGN KEY (DEPARTURE_DATE_KEY) REFERENCES CORE.DIM_DATE      (DATE_KEY)
);

-- Not append-only, unlike the RAW streams: the fact is loaded by MERGE, so a
-- correction arrives as an update and MART has to recompute that cell.
CREATE STREAM IF NOT EXISTS CORE.STRM_FCT_BOOKING_MART
    ON TABLE CORE.FCT_FLIGHT_BOOKING;
