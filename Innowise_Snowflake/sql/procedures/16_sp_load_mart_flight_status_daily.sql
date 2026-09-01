-- Stage 2 -> stage 3. Incremental aggregate.
--
-- An aggregate cannot be built from a CDC feed by adding up the changed rows —
-- an update would be double-counted. So the stream is used only to answer
-- "which (date, continent) cells were touched", and those cells are then
-- recomputed from scratch against the fact table.
--
-- Both halves are one MERGE on purpose: the stream is read exactly once, in the
-- same statement that writes the result, so it cannot advance past work that
-- then fails.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE MART.SP_LOAD_AGG_FLIGHT_STATUS_DAILY(P_RUN_ID STRING)
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
    MERGE INTO MART.AGG_FLIGHT_STATUS_DAILY t
    USING (
        SELECT f.DEPARTURE_DATE_KEY,
               d.FULL_DATE          AS DEPARTURE_DATE,
               a.CONTINENT_CODE,
               a.CONTINENT_NAME,
               f.FLIGHT_STATUS,
               COUNT(*)             AS BOOKINGS_CNT,
               ROUND(AVG(p.AGE), 2) AS AVG_PASSENGER_AGE
        FROM CORE.FCT_FLIGHT_BOOKING f
        JOIN CORE.DIM_AIRPORT   a ON a.AIRPORT_KEY   = f.AIRPORT_KEY
        JOIN CORE.DIM_DATE      d ON d.DATE_KEY      = f.DEPARTURE_DATE_KEY
        JOIN CORE.DIM_PASSENGER p ON p.PASSENGER_KEY = f.PASSENGER_KEY
        -- The stream, reduced to the set of cells that need rebuilding.
        JOIN (
            SELECT DISTINCT c.DEPARTURE_DATE_KEY, ca.CONTINENT_CODE
            FROM CORE.STRM_FCT_BOOKING_MART c
            JOIN CORE.DIM_AIRPORT ca ON ca.AIRPORT_KEY = c.AIRPORT_KEY
        ) chg ON chg.DEPARTURE_DATE_KEY = f.DEPARTURE_DATE_KEY
             AND chg.CONTINENT_CODE     = a.CONTINENT_CODE
        GROUP BY 1, 2, 3, 4, 5
    ) s
    ON  t.DEPARTURE_DATE_KEY = s.DEPARTURE_DATE_KEY
    AND t.CONTINENT_CODE     = s.CONTINENT_CODE
    AND t.FLIGHT_STATUS      = s.FLIGHT_STATUS
    WHEN MATCHED AND (t.BOOKINGS_CNT      <> s.BOOKINGS_CNT
                   OR t.AVG_PASSENGER_AGE IS DISTINCT FROM s.AVG_PASSENGER_AGE) THEN UPDATE SET
        t.BOOKINGS_CNT = s.BOOKINGS_CNT, t.AVG_PASSENGER_AGE = s.AVG_PASSENGER_AGE,
        t.DWH_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (DEPARTURE_DATE_KEY, DEPARTURE_DATE, CONTINENT_CODE, CONTINENT_NAME,
         FLIGHT_STATUS, BOOKINGS_CNT, AVG_PASSENGER_AGE, DWH_UPDATED_AT)
        VALUES
        (s.DEPARTURE_DATE_KEY, s.DEPARTURE_DATE, s.CONTINENT_CODE, s.CONTINENT_NAME,
         s.FLIGHT_STATUS, s.BOOKINGS_CNT, s.AVG_PASSENGER_AGE, CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated"
      INTO :V_INS, :V_UPD FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'MART.AGG_FLIGHT_STATUS_DAILY', 'MERGE',
                             :V_INS, :V_UPD, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'MART.AGG_FLIGHT_STATUS_DAILY', 'MERGE',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
