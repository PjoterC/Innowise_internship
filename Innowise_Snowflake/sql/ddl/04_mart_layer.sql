-- MART — stage 3. What a consumer reads. Nothing in RAW, CORE or META is ever
-- granted to a consumer role; the secure view is the whole public surface.

USE DATABASE AIRLINE_DWH;

-- Grain: departure date x continent x flight status. Deliberately the grain the
-- fact's stream can identify cheaply, so the loader recomputes only the cells
-- that actually changed.
CREATE TABLE IF NOT EXISTS MART.AGG_FLIGHT_STATUS_DAILY (
    DEPARTURE_DATE_KEY NUMBER(8,0)   NOT NULL,
    DEPARTURE_DATE     DATE,
    CONTINENT_CODE     VARCHAR       NOT NULL,
    CONTINENT_NAME     VARCHAR,
    FLIGHT_STATUS      VARCHAR       NOT NULL,
    BOOKINGS_CNT       NUMBER(38,0)  NOT NULL,
    AVG_PASSENGER_AGE  NUMBER(10,2),
    DWH_UPDATED_AT     TIMESTAMP_LTZ NOT NULL,
    CONSTRAINT PK_AGG_FLIGHT_STATUS_DAILY
        PRIMARY KEY (DEPARTURE_DATE_KEY, CONTINENT_CODE, FLIGHT_STATUS)
);

-- ---------------------------------------------------------------------------
-- Secure view + row access policy
-- ---------------------------------------------------------------------------
-- Two separate mechanisms, doing two different jobs:
--
--   SECURE hides the view's definition from anyone who is not its owner, and
--   stops the optimiser pushing a user-supplied predicate below the view's own
--   filters — which is how a cleverly written WHERE clause can otherwise infer
--   rows it was never shown. It costs some optimisations; that is the price.
--
--   The ROW ACCESS POLICY does the filtering, declared once in META and
--   attached below. Keeping it out of the view body means the rule is a
--   first-class object: it can be attached to a second view or to the base
--   table without being rewritten, and SHOW / POLICY_REFERENCES can list
--   everywhere it applies.


CREATE OR REPLACE SECURE VIEW MART.V_FLIGHT_BOOKING_SECURE
    COMMENT = 'The fact, denormalised. Guarded by META.RAP_CONTINENT.'
AS
SELECT f.BOOKING_ID,
       d.FULL_DATE AS DEPARTURE_DATE,
       d.MONTH_NAME,
       d.YEAR_NUMBER,
       d.IS_WEEKEND,
       a.AIRPORT_CODE,
       a.AIRPORT_NAME,
       a.COUNTRY_NAME,
       a.CONTINENT_CODE,
       a.CONTINENT_NAME,
       p.PASSENGER_ID,
       p.FIRST_NAME || ' ' || p.LAST_NAME AS PASSENGER_NAME,
       p.GENDER,
       p.AGE,
       p.NATIONALITY,
       f.FLIGHT_STATUS,
       f.IS_ON_TIME,
       f.IS_DELAYED,
       f.IS_CANCELLED
FROM CORE.FCT_FLIGHT_BOOKING f
JOIN CORE.DIM_AIRPORT   a ON a.AIRPORT_KEY   = f.AIRPORT_KEY
JOIN CORE.DIM_PASSENGER p ON p.PASSENGER_KEY = f.PASSENGER_KEY
JOIN CORE.DIM_DATE      d ON d.DATE_KEY      = f.DEPARTURE_DATE_KEY;

-- Must follow the CREATE above, and must not be merged into it: a view cannot
-- declare its own policy. CREATE OR REPLACE drops the old view along with its
-- attachment, so re-running this file re-attaches cleanly rather than failing
-- with "already has a row access policy".
ALTER VIEW MART.V_FLIGHT_BOOKING_SECURE
    ADD ROW ACCESS POLICY META.RAP_CONTINENT ON (CONTINENT_CODE);
