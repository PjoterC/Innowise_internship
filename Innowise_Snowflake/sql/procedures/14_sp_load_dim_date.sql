-- Stage 1 -> stage 2. The calendar is densified from the dates the source
-- actually contains, so it holds only days a fact points at.
--
-- The MERGE keeps a WHEN MATCHED clause it will essentially never fire, because
-- RESULT_SCAN only returns a "number of rows updated" column when the statement
-- has one — and the audit call reads that column unconditionally.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_DIM_DATE(P_RUN_ID STRING)
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
    MERGE INTO CORE.DIM_DATE t
    USING (
        SELECT TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD')) AS DATE_KEY,
               FULL_DATE,
               DAYNAME(FULL_DATE)                        AS DAY_NAME,
               DAYOFWEEKISO(FULL_DATE) >= 6              AS IS_WEEKEND,
               MONTH(FULL_DATE)                          AS MONTH_NUMBER,
               MONTHNAME(FULL_DATE)                      AS MONTH_NAME,
               QUARTER(FULL_DATE)                        AS QUARTER_NUMBER,
               YEAR(FULL_DATE)                           AS YEAR_NUMBER
        FROM (
            SELECT DISTINCT TRY_TO_DATE(TRIM(DEPARTURE_DATE), 'MM/DD/YYYY') AS FULL_DATE
            FROM RAW.STRM_AIRLINE_RAW_DIM_DATE
        )
        WHERE FULL_DATE IS NOT NULL
    ) s
    ON t.DATE_KEY = s.DATE_KEY
    WHEN MATCHED AND t.FULL_DATE IS DISTINCT FROM s.FULL_DATE THEN UPDATE SET
        t.FULL_DATE = s.FULL_DATE
    WHEN NOT MATCHED THEN INSERT
        (DATE_KEY, FULL_DATE, DAY_NAME, IS_WEEKEND, MONTH_NUMBER, MONTH_NAME,
         QUARTER_NUMBER, YEAR_NUMBER, DWH_INSERTED_AT)
        VALUES
        (s.DATE_KEY, s.FULL_DATE, s.DAY_NAME, s.IS_WEEKEND, s.MONTH_NUMBER, s.MONTH_NAME,
         s.QUARTER_NUMBER, s.YEAR_NUMBER, CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated"
      INTO :V_INS, :V_UPD FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_DATE', 'MERGE',
                             :V_INS, :V_UPD, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_DATE', 'MERGE',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
