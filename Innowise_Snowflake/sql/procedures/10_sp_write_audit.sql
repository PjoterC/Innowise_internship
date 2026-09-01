-- The single writer of the audit log. Every load procedure calls exactly this,
-- on success and on failure, so the table has one shape and one producer.
-- FINISHED_AT and DURATION_SEC are computed here, not passed in, so a caller
-- cannot report a duration that disagrees with its own timestamps.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE META.SP_WRITE_AUDIT(
    P_RUN_ID        STRING,
    P_PIPELINE_NAME STRING,
    P_TARGET_OBJECT STRING,
    P_OPERATION     STRING,
    P_ROWS_INSERTED NUMBER,
    P_ROWS_UPDATED  NUMBER,
    P_STARTED_AT    TIMESTAMP_LTZ,
    P_STATUS        STRING,
    P_ERROR_MESSAGE STRING,
    P_QUERY_ID      STRING
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO META.ETL_AUDIT_LOG (
        RUN_ID, PIPELINE_NAME, TARGET_OBJECT, OPERATION,
        ROWS_INSERTED, ROWS_UPDATED,
        STARTED_AT, FINISHED_AT, DURATION_SEC,
        STATUS, ERROR_MESSAGE, QUERY_ID)
    SELECT :P_RUN_ID, :P_PIPELINE_NAME, :P_TARGET_OBJECT, :P_OPERATION,
           COALESCE(:P_ROWS_INSERTED, 0), COALESCE(:P_ROWS_UPDATED, 0),
           :P_STARTED_AT, CURRENT_TIMESTAMP(),
           DATEDIFF('millisecond', :P_STARTED_AT, CURRENT_TIMESTAMP()) / 1000.0,
           :P_STATUS,
           -- Truncated: a Snowflake error can be longer than the column, and
           -- losing the audit row to a truncation error would be the worst
           -- possible failure mode for a logger.
           LEFT(:P_ERROR_MESSAGE, 5000),
           :P_QUERY_ID;
    RETURN SQLROWCOUNT;
END;
$$;
