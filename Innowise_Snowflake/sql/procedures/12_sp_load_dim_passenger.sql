-- Stage 1 -> stage 2, SCD type 1.
--
-- The shape below is shared by every CORE/MART loader:
--   1. one MERGE reading one stream — one statement, so it either commits with
--      the stream consumed or fails with the stream untouched, and a retried
--      task picks up exactly the rows it missed;
--   2. SQLID captures that MERGE's query id, RESULT_SCAN turns it into the
--      insert/update counts the audit log wants;
--   3. SP_WRITE_AUDIT on both paths, then RAISE so the Airflow task still fails.
--
-- QUALIFY is not optional: a batch can contain the same passenger twice, and
-- MERGE raises "duplicate row detected" if two source rows match one target row.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_DIM_PASSENGER(P_RUN_ID STRING)
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
    MERGE INTO CORE.DIM_PASSENGER t
    USING (
        SELECT MD5(TRIM(PASSENGER_ID))     AS PASSENGER_KEY,
               TRIM(PASSENGER_ID)          AS PASSENGER_ID,
               TRIM(FIRST_NAME)            AS FIRST_NAME,
               TRIM(LAST_NAME)             AS LAST_NAME,
               TRIM(GENDER)                AS GENDER,
               TRY_TO_NUMBER(TRIM(AGE))    AS AGE,
               TRIM(NATIONALITY)           AS NATIONALITY,
               MD5(CONCAT_WS('|', TRIM(FIRST_NAME), TRIM(LAST_NAME), TRIM(GENDER),
                                  TRIM(AGE), TRIM(NATIONALITY))) AS RECORD_HASH
        FROM RAW.STRM_AIRLINE_RAW_DIM_PASSENGER
        WHERE TRIM(PASSENGER_ID) IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(PASSENGER_ID)
                                   ORDER BY LOAD_TS DESC, SRC_FILE_ROW_NUMBER DESC) = 1
    ) s
    ON t.PASSENGER_KEY = s.PASSENGER_KEY
    -- Hash guard: unchanged rows are not rewritten, so the audit log reports
    -- real work rather than the batch size.
    WHEN MATCHED AND t.RECORD_HASH <> s.RECORD_HASH THEN UPDATE SET
        t.FIRST_NAME = s.FIRST_NAME, t.LAST_NAME = s.LAST_NAME, t.GENDER = s.GENDER,
        t.AGE = s.AGE, t.NATIONALITY = s.NATIONALITY, t.RECORD_HASH = s.RECORD_HASH,
        t.DWH_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (PASSENGER_KEY, PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE,
         NATIONALITY, RECORD_HASH, DWH_INSERTED_AT, DWH_UPDATED_AT)
        VALUES
        (s.PASSENGER_KEY, s.PASSENGER_ID, s.FIRST_NAME, s.LAST_NAME, s.GENDER, s.AGE,
         s.NATIONALITY, s.RECORD_HASH, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated"
      INTO :V_INS, :V_UPD FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_PASSENGER', 'MERGE',
                             :V_INS, :V_UPD, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_PASSENGER', 'MERGE',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
