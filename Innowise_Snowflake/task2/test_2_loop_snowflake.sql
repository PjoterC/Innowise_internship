-- =============================================================================
-- Task 2 - Snowflake solution
--
--   arg1 (batch date)  - data, must be listed
--   arg2 (loaded_at)   - data, must be listed; no formula relates it to arg1
--   arg3 (prev batch)  - DERIVED: it is always arg1 of the preceding call,
--                        i.e. LAG(arg1) over the calls ordered by arg1.
--                        The very first call has no predecessor, so it needs a
--                        seed value ('2025-01-28').
--
-- Snowflake Scripting has no FOR-over-a-query, so the driver query is held in a
-- RESULTSET and iterated through a CURSOR.
-- =============================================================================

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE RUN_TEST_2_BATCHES(P_SEED_PREV DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    RS        RESULTSET;
    V_ARG1    STRING;
    V_ARG2    STRING;
    V_ARG3    STRING;
    V_CALLS   NUMBER := 0;
BEGIN
    RS := (
        SELECT TO_CHAR(BATCH_DATE, 'YYYY-MM-DD')                AS ARG1,
               TO_CHAR(LOADED_AT, 'YYYY-MM-DD HH24:MI:SS.FF9')  AS ARG2,
               -- COALESCE rather than the three-argument LAG: the seed arrives
               -- as a bind variable, and a bind is not guaranteed to be
               -- accepted in LAG's default slot. BATCH_DATE is never NULL, so
               -- the only NULL LAG can produce is the out-of-range first row.
               TO_CHAR(COALESCE(LAG(BATCH_DATE) OVER (ORDER BY BATCH_DATE),
                                :P_SEED_PREV), 'YYYY-MM-DD')    AS ARG3
        FROM VALUES
            ('2025-02-06'::DATE, '2025-02-12 09:38:25.999982000'::TIMESTAMP_NTZ),
            ('2025-02-14'::DATE, '2025-02-14 16:17:14.095384000'::TIMESTAMP_NTZ),
            ('2025-02-20'::DATE, '2025-02-21 08:41:53.643244000'::TIMESTAMP_NTZ),
            ('2025-02-25'::DATE, '2025-03-11 15:52:28.575590000'::TIMESTAMP_NTZ),
            ('2025-03-06'::DATE, '2025-03-13 15:35:21.729785000'::TIMESTAMP_NTZ),
            ('2025-03-13'::DATE, '2025-03-13 16:32:27.178218000'::TIMESTAMP_NTZ),
            ('2025-03-20'::DATE, '2025-03-26 08:35:19.585812000'::TIMESTAMP_NTZ),
            ('2025-03-27'::DATE, '2025-03-28 07:23:03.611707000'::TIMESTAMP_NTZ),
            ('2025-04-07'::DATE, '2025-04-08 18:57:03.804270000'::TIMESTAMP_NTZ),
            ('2025-04-10'::DATE, '2025-04-15 11:19:51.275211000'::TIMESTAMP_NTZ),
            ('2025-04-14'::DATE, '2025-04-15 14:34:32.097939000'::TIMESTAMP_NTZ),
            ('2025-04-24'::DATE, '2025-04-24 14:41:48.705573000'::TIMESTAMP_NTZ),
            ('2025-05-02'::DATE, '2025-05-08 11:05:44.640510000'::TIMESTAMP_NTZ),
            ('2025-05-15'::DATE, '2025-05-21 10:00:08.361011000'::TIMESTAMP_NTZ),
            ('2025-05-22'::DATE, '2025-05-28 08:07:06.096731000'::TIMESTAMP_NTZ),
            ('2025-05-29'::DATE, '2025-05-30 10:01:45.906511000'::TIMESTAMP_NTZ),
            ('2025-06-05'::DATE, '2025-06-09 09:22:04.668390000'::TIMESTAMP_NTZ),
            ('2025-06-19'::DATE, '2025-07-03 08:27:40.115104000'::TIMESTAMP_NTZ),
            ('2025-06-26'::DATE, '2025-07-03 09:15:38.292950000'::TIMESTAMP_NTZ),
            ('2025-07-03'::DATE, '2025-07-07 10:53:30.915895000'::TIMESTAMP_NTZ)
            AS V(BATCH_DATE, LOADED_AT)
        ORDER BY BATCH_DATE
    );

    LET C CURSOR FOR RS;

    FOR REC IN C DO
        -- Copied into scalars first: a CALL argument has to be a bind
        -- variable or a literal, and REC.ARG1 is neither.
        V_ARG1 := REC.ARG1;
        V_ARG2 := REC.ARG2;
        V_ARG3 := REC.ARG3;

       
        CALL TEST_2(:V_ARG1, :V_ARG2, :V_ARG3);

        -- If TEST_2 is declared as (DATE, TIMESTAMP_NTZ, DATE), drop the
        -- TO_CHAR wrappers in the driver query, declare V_ARG1/V_ARG3 as DATE
        -- and V_ARG2 as TIMESTAMP_NTZ, and keep this CALL unchanged.

        V_CALLS := V_CALLS + 1;
    END FOR;

    RETURN V_CALLS || ' calls to TEST_2 completed';
END;
$$;


-- Run it:
CALL RUN_TEST_2_BATCHES('2025-01-28'::DATE);

