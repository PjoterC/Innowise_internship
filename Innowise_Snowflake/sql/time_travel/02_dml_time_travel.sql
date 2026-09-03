-- Two DML statements that use Time Travel.
--

USE DATABASE AIRLINE_DWH;

-- --- DML 1: what did the last passenger load actually change? ---------------
-- AT / BEFORE take a literal or a session variable, not a subquery, so the
-- query id is fetched into a variable first.
SET merge_qid = (
    SELECT QUERY_ID FROM META.ETL_AUDIT_LOG
    WHERE TARGET_OBJECT = 'CORE.DIM_PASSENGER' AND STATUS = 'SUCCESS' AND QUERY_ID IS NOT NULL
    ORDER BY AUDIT_ID DESC LIMIT 1
);

SELECT 'before load' AS snapshot, COUNT(*) AS passengers
FROM CORE.DIM_PASSENGER BEFORE (STATEMENT => $merge_qid)
UNION ALL
SELECT 'after load', COUNT(*)
FROM CORE.DIM_PASSENGER;

----- DML 2: undo a DELETE ---------------------------------------

SELECT COUNT(*) AS japanese_passengers_before FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

DELETE FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';
SET delete_qid = LAST_QUERY_ID();

SELECT COUNT(*) AS japanese_passengers_after_delete FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

INSERT INTO CORE.DIM_PASSENGER
SELECT * FROM CORE.DIM_PASSENGER BEFORE (STATEMENT => $delete_qid)
WHERE NATIONALITY = 'Japan';

SELECT COUNT(*) AS japanese_passengers_restored FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';
