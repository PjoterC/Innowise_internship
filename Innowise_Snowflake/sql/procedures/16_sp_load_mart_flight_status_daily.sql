-- Stage 2 -> stage 3. Incremental aggregate.
--
-- An aggregate cannot be built from a CDC feed by adding up the changed rows —
-- an update would be double-counted. So the stream is used only to answer
-- "which (date, continent) cells were touched", and those cells are then
-- recomputed from scratch against the fact table.
--
-- Recomputing is not enough on its own. A fact row that moves to another date
-- or another status empties the cell it left, and a cell with no fact rows
-- produces no group — so a plain "recompute what changed" MERGE has nothing to
-- match against the stale target row and silently leaves the old count in
-- place. The EMPTIED branch below manufactures the missing row with a count of
-- zero, which the first WHEN MATCHED clause then turns into a DELETE.
--
-- All of it is one MERGE on purpose: the stream is read only inside the
-- statement that writes the result, so its offset cannot advance past work that
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
    V_DEL     NUMBER := 0;
BEGIN
    MERGE INTO MART.AGG_FLIGHT_STATUS_DAILY t
    USING (
        -- The CTEs sit inside USING (...) because that is the only place
        -- Snowflake accepts them here: a WITH clause may open a subquery, but
        -- it may not precede MERGE the way it can precede SELECT.
        --
        -- CHG is the stream, reduced to the set of cells that need rebuilding.
        -- An update arrives as a DELETE row carrying the old airport and an
        -- INSERT row carrying the new one, so a booking that moves between
        -- continents puts both cells in here.
        WITH CHG AS (
            SELECT DISTINCT c.DEPARTURE_DATE_KEY, ca.CONTINENT_CODE
            FROM CORE.STRM_FCT_BOOKING_MART c
            JOIN CORE.DIM_AIRPORT ca ON ca.AIRPORT_KEY = c.AIRPORT_KEY
        ),
        -- What those cells look like now, straight from the fact.
        RECOMPUTED AS (
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
            JOIN CHG ON CHG.DEPARTURE_DATE_KEY = f.DEPARTURE_DATE_KEY
                    AND CHG.CONTINENT_CODE     = a.CONTINENT_CODE
            GROUP BY 1, 2, 3, 4, 5
        ),
        -- Rows the mart still holds inside a touched cell that the fact no
        -- longer produces. Scoped to CHG, so an untouched cell is never
        -- considered and a partial rebuild cannot delete the rest of the table.
        EMPTIED AS (
            SELECT e.DEPARTURE_DATE_KEY,
                   e.DEPARTURE_DATE,
                   e.CONTINENT_CODE,
                   e.CONTINENT_NAME,
                   e.FLIGHT_STATUS,
                   0                          AS BOOKINGS_CNT,
                   CAST(NULL AS NUMBER(10,2)) AS AVG_PASSENGER_AGE
            FROM MART.AGG_FLIGHT_STATUS_DAILY e
            JOIN CHG ON CHG.DEPARTURE_DATE_KEY = e.DEPARTURE_DATE_KEY
                    AND CHG.CONTINENT_CODE     = e.CONTINENT_CODE
            WHERE NOT EXISTS (
                SELECT 1 FROM RECOMPUTED r
                WHERE r.DEPARTURE_DATE_KEY = e.DEPARTURE_DATE_KEY
                  AND r.CONTINENT_CODE     = e.CONTINENT_CODE
                  AND r.FLIGHT_STATUS      = e.FLIGHT_STATUS)
        )
        SELECT * FROM RECOMPUTED
        UNION ALL
        SELECT * FROM EMPTIED
    ) s
    ON  t.DEPARTURE_DATE_KEY = s.DEPARTURE_DATE_KEY
    AND t.CONTINENT_CODE     = s.CONTINENT_CODE
    AND t.FLIGHT_STATUS      = s.FLIGHT_STATUS
    -- Ordered: the zero-count rows only ever come from EMPTIED, and EMPTIED
    -- only ever produces rows that already exist in the target.
    WHEN MATCHED AND s.BOOKINGS_CNT = 0 THEN DELETE
    WHEN MATCHED AND (t.BOOKINGS_CNT      <> s.BOOKINGS_CNT
                   OR t.AVG_PASSENGER_AGE IS DISTINCT FROM s.AVG_PASSENGER_AGE) THEN UPDATE SET
        t.BOOKINGS_CNT = s.BOOKINGS_CNT, t.AVG_PASSENGER_AGE = s.AVG_PASSENGER_AGE,
        t.DWH_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND s.BOOKINGS_CNT > 0 THEN INSERT
        (DEPARTURE_DATE_KEY, DEPARTURE_DATE, CONTINENT_CODE, CONTINENT_NAME,
         FLIGHT_STATUS, BOOKINGS_CNT, AVG_PASSENGER_AGE, DWH_UPDATED_AT)
        VALUES
        (s.DEPARTURE_DATE_KEY, s.DEPARTURE_DATE, s.CONTINENT_CODE, s.CONTINENT_NAME,
         s.FLIGHT_STATUS, s.BOOKINGS_CNT, s.AVG_PASSENGER_AGE, CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated", "number of rows deleted"
      INTO :V_INS, :V_UPD, :V_DEL FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'MART.AGG_FLIGHT_STATUS_DAILY', 'MERGE',
                             :V_INS, :V_UPD, :V_DEL, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD || ' deleted=' || V_DEL;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'MART.AGG_FLIGHT_STATUS_DAILY', 'MERGE',
                                 0, 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
