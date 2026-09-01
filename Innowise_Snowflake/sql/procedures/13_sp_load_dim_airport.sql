-- Stage 1 -> stage 2, SCD type 1. Same shape as 12_sp_load_dim_passenger.sql.
--
-- The natural key is the triple code + name + country because neither column is
-- unique on its own in this source: 25 IATA codes carry more than one airport
-- name and 57 names carry more than one code. The key expression here must stay
-- character-for-character identical to the one in the fact loader, which
-- computes the same hash instead of looking this table up.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE CORE.SP_LOAD_DIM_AIRPORT(P_RUN_ID STRING)
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
    MERGE INTO CORE.DIM_AIRPORT t
    USING (
        SELECT MD5(CONCAT_WS('|', TRIM(ARRIVAL_AIRPORT), TRIM(AIRPORT_NAME),
                                  TRIM(AIRPORT_COUNTRY_CODE))) AS AIRPORT_KEY,
               TRIM(ARRIVAL_AIRPORT)      AS AIRPORT_CODE,
               TRIM(AIRPORT_NAME)         AS AIRPORT_NAME,
               TRIM(AIRPORT_COUNTRY_CODE) AS COUNTRY_CODE,
               TRIM(COUNTRY_NAME)         AS COUNTRY_NAME,
               TRIM(AIRPORT_CONTINENT)    AS CONTINENT_CODE,
               TRIM(CONTINENTS)           AS CONTINENT_NAME,
               MD5(CONCAT_WS('|', TRIM(COUNTRY_NAME), TRIM(AIRPORT_CONTINENT),
                                  TRIM(CONTINENTS))) AS RECORD_HASH
        FROM RAW.STRM_AIRLINE_RAW_DIM_AIRPORT
        WHERE TRIM(ARRIVAL_AIRPORT) IS NOT NULL AND TRIM(AIRPORT_NAME) IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY MD5(CONCAT_WS('|', TRIM(ARRIVAL_AIRPORT), TRIM(AIRPORT_NAME),
                                            TRIM(AIRPORT_COUNTRY_CODE)))
            ORDER BY LOAD_TS DESC, SRC_FILE_ROW_NUMBER DESC) = 1
    ) s
    ON t.AIRPORT_KEY = s.AIRPORT_KEY
    WHEN MATCHED AND t.RECORD_HASH <> s.RECORD_HASH THEN UPDATE SET
        t.COUNTRY_NAME = s.COUNTRY_NAME, t.CONTINENT_CODE = s.CONTINENT_CODE,
        t.CONTINENT_NAME = s.CONTINENT_NAME, t.RECORD_HASH = s.RECORD_HASH,
        t.DWH_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (AIRPORT_KEY, AIRPORT_CODE, AIRPORT_NAME, COUNTRY_CODE, COUNTRY_NAME,
         CONTINENT_CODE, CONTINENT_NAME, RECORD_HASH, DWH_INSERTED_AT, DWH_UPDATED_AT)
        VALUES
        (s.AIRPORT_KEY, s.AIRPORT_CODE, s.AIRPORT_NAME, s.COUNTRY_CODE, s.COUNTRY_NAME,
         s.CONTINENT_CODE, s.CONTINENT_NAME, s.RECORD_HASH, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    V_QID := SQLID;
    SELECT "number of rows inserted", "number of rows updated"
      INTO :V_INS, :V_UPD FROM TABLE(RESULT_SCAN(:V_QID));

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_AIRPORT', 'MERGE',
                             :V_INS, :V_UPD, :V_STARTED, 'SUCCESS', NULL, :V_QID);
    RETURN 'inserted=' || V_INS || ' updated=' || V_UPD;
EXCEPTION
    WHEN OTHER THEN
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_pipeline', 'CORE.DIM_AIRPORT', 'MERGE',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
