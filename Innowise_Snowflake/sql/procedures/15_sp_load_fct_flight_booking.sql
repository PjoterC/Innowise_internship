-- Stage 1 -> stage 2, the fact. Runs after all three dimensions.
--
-- The dimension keys are recomputed here with the same MD5 expressions the
-- dimension loaders use, rather than joined out of the dimension tables. That
-- is the whole reason for hash keys: no lookup, no missing-key fallback, and
-- the fact is correct even if a dimension row lands in the same batch.
--
-- An unparseable departure date resolves to the -1 unknown member instead of
-- NULL, so MART's joins can stay INNER.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_FCT_FLIGHT_BOOKING(P_RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_STARTED TIMESTAMP_LTZ := CURRENT_TIMESTAMP();
    V_QID     STRING;
    V_INS     NUMBER := 0;
    V_UPD     NUMBER := 0;
BEGIN
    MERGE INTO CORE.FCT_FLIGHT_BOOKING t
    USING (
        SELECT MD5(TRIM(SOURCE_ROW_ID))              AS BOOKING_KEY,
               TRY_TO_NUMBER(TRIM(SOURCE_ROW_ID))    AS BOOKING_ID,
               MD5(TRIM(PASSENGER_ID))               AS PASSENGER_KEY,
               MD5(CONCAT_WS('|', TRIM(ARRIVAL_AIRPORT), TRIM(AIRPORT_NAME),
                                  TRIM(AIRPORT_COUNTRY_CODE))) AS AIRPORT_KEY,
               COALESCE(TO_NUMBER(TO_CHAR(
                   TRY_TO_DATE(TRIM(DEPARTURE_DATE), 'MM/DD/YYYY'), 'YYYYMMDD')), -1)
                                                     AS DEPARTURE_DATE_KEY,
               TRIM(FLIGHT_STATUS)                   AS FLIGHT_STATUS,
               TRIM(TICKET_TYPE)                     AS TICKET_TYPE,
               TRIM(PASSENGER_STATUS)                AS PASSENGER_STATUS,
               IFF(TRIM(FLIGHT_STATUS) = 'On Time',   1, 0) AS IS_ON_TIME,
               IFF(TRIM(FLIGHT_STATUS) = 'Delayed',   1, 0) AS IS_DELAYED,
               IFF(TRIM(FLIGHT_STATUS) = 'Cancelled', 1, 0) AS IS_CANCELLED,
               MD5(CONCAT_WS('|', TRIM(PASSENGER_ID), TRIM(ARRIVAL_AIRPORT),
                                  TRIM(DEPARTURE_DATE), TRIM(FLIGHT_STATUS),
                                  TRIM(TICKET_TYPE), TRIM(PASSENGER_STATUS))) AS RECORD_HASH
        FROM RAW.STRM_AIRLINE_RAW_FCT_BOOKING
        WHERE TRY_TO_NUMBER(TRIM(SOURCE_ROW_ID)) IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(SOURCE_ROW_ID)
                                   ORDER BY LOAD_TS DESC, SRC_FILE_ROW_NUMBER DESC) = 1
    ) s
    ON t.BOOKING_KEY = s.BOOKING_KEY
    WHEN MATCHED AND t.RECORD_HASH <> s.RECORD_HASH THEN UPDATE SET
        t.PASSENGER_KEY = s.PASSENGER_KEY, t.AIRPORT_KEY = s.AIRPORT_KEY,
        t.DEPARTURE_DATE_KEY = s.DEPARTURE_DATE_KEY, t.FLIGHT_STATUS = s.FLIGHT_STATUS,
        t.TICKET_TYPE = s.TICKET_TYPE, t.PASSENGER_STATUS = s.PASSENGER_STATUS,
        t.IS_ON_TIME = s.IS_ON_TIME, t.IS_DELAYED = s.IS_DELAYED,
        t.IS_CANCELLED = s.IS_CANCELLED, t.RECORD_HASH = s.RECORD_HASH,
        t.DWH_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (BOOKING_KEY, BOOKING_ID, PASSENGER_KEY, AIRPORT_KEY, DEPARTURE_DATE_KEY,
         FLIGHT_STATUS, TICKET_TYPE, PASSENGER_STATUS,
         IS_ON_TIME, IS_DELAYED, IS_CANCELLED,
         RECORD_HASH, DWH_INSERTED_AT, DWH_UPDATED_AT)
        VALUES
        (s.BOOKING_KEY, s.BOOKING_ID, s.PASSENGER_KEY, s.AIRPORT_KEY, s.DEPARTURE_DATE_KEY,
         s.FLIGHT_STATUS, s.TICKET_TYPE, s.PASSENGER_STATUS,
         s.IS_ON_TIME, s.IS_DELAYED, s.IS_CANCELLED,
         s.RECORD_HASH, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated"
      INTO :V_INS, :V_UPD FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.FCT_FLIGHT_BOOKING', 'MERGE',
                             :V_INS, :V_UPD, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.FCT_FLIGHT_BOOKING', 'MERGE',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
