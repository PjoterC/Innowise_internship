-- Two DML statements that use Time Travel: one undoes an UPDATE, one undoes a
-- DELETE. Each is preceded by the statement that does the damage and followed
-- by a count, so the file runs top to bottom and every repair has something
-- real to repair.
--
-- Both address their snapshot with BEFORE (STATEMENT => <query id>) — the same
-- clause the context block below feeds from META.ETL_AUDIT_LOG. That is what
-- QUERY_ID is doing in the audit table: the id of any logged load can name the
-- moment just before that load ran, which turns "the pipeline wrote something
-- wrong at 03:00" into an addressable version of the table. The repairs
-- themselves use LAST_QUERY_ID(), because the statement each one undoes was run
-- seconds earlier in this same session.
--
-- Everything here has to fall inside DATA_RETENTION_TIME_IN_DAYS (7, set in
-- 00_database_and_schemas.sql) and inside the table's own lifetime.

USE DATABASE AIRLINE_DWH;

-- --- Context: addressing a snapshot from the audit log ----------------------
-- Not one of the two DML statements — it is the lookup they would use in anger,
-- shown once on its own. AT / BEFORE take a literal or a session variable and
-- not a subquery, so the query id is fetched into a variable first.
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

-- --- DML 1: undo an accidental UPDATE ---------------------------------------
-- The damage. Note what makes this the nastier of the two accidents: the row
-- count does not change, and RECORD_HASH still holds the hash of the *old*
-- values — so the dimension now disagrees with its own change-detection column,
-- and the next pipeline run would compare hashes, find them equal, and leave
-- the corruption in place. Nothing downstream would ever report it.
SELECT COUNT(*) AS japanese_before_update FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

UPDATE CORE.DIM_PASSENGER SET NATIONALITY = 'UNKNOWN' WHERE NATIONALITY = 'Japan';
SET update_qid = LAST_QUERY_ID();

SELECT COUNT(*) AS japanese_after_update FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';


UPDATE CORE.DIM_PASSENGER t
   SET NATIONALITY    = b.NATIONALITY,
       DWH_UPDATED_AT = CURRENT_TIMESTAMP()
  FROM CORE.DIM_PASSENGER BEFORE (STATEMENT => $update_qid) AS b
 WHERE t.PASSENGER_KEY = b.PASSENGER_KEY
   AND t.NATIONALITY IS DISTINCT FROM b.NATIONALITY;

SELECT COUNT(*) AS japanese_after_restore FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

-- --- DML 2: undo a DELETE ---------------------------------------------------
-- The same idea against rows that are gone rather than wrong. A DELETE leaves
-- nothing to join to, so the snapshot supplies the rows themselves and the
-- repair is an INSERT.
SELECT COUNT(*) AS japanese_passengers_before FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

DELETE FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';
SET delete_qid = LAST_QUERY_ID();

SELECT COUNT(*) AS japanese_passengers_after_delete FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';

INSERT INTO CORE.DIM_PASSENGER
SELECT * FROM CORE.DIM_PASSENGER BEFORE (STATEMENT => $delete_qid)
WHERE NATIONALITY = 'Japan';

SELECT COUNT(*) AS japanese_passengers_restored FROM CORE.DIM_PASSENGER WHERE NATIONALITY = 'Japan';
