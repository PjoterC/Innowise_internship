-- META — the pipeline's own bookkeeping.

USE DATABASE AIRLINE_DWH;

-- One row per load step per DAG run, so the log can answer "which step wrote
-- how much". QUERY_ID earns its column twice over: it joins back to
-- QUERY_HISTORY, and it is what the Time Travel scripts hand to
-- BEFORE (STATEMENT => ...) to see a table as it was before that step ran.
CREATE TABLE IF NOT EXISTS META.ETL_AUDIT_LOG (
    AUDIT_ID       NUMBER(38,0)  IDENTITY(1,1),
    RUN_ID         VARCHAR(250)  NOT NULL,   -- Airflow dag_run.run_id
    PIPELINE_NAME  VARCHAR(100)  NOT NULL,
    TARGET_OBJECT  VARCHAR(200)  NOT NULL,
    OPERATION      VARCHAR(20)   NOT NULL,   -- COPY | MERGE
    ROWS_INSERTED  NUMBER(38,0)  DEFAULT 0,
    ROWS_UPDATED   NUMBER(38,0)  DEFAULT 0,
    STARTED_AT     TIMESTAMP_LTZ NOT NULL,
    FINISHED_AT    TIMESTAMP_LTZ NOT NULL,
    DURATION_SEC   NUMBER(18,3),
    STATUS         VARCHAR(20)   NOT NULL,   -- SUCCESS | FAILED
    ERROR_MESSAGE  VARCHAR(5000),
    QUERY_ID       VARCHAR(64),
    CONSTRAINT PK_ETL_AUDIT_LOG PRIMARY KEY (AUDIT_ID)
);

-- Row-level security as data: who sees what is a row here, not a redeploy.
-- '*' means every continent.
CREATE TABLE IF NOT EXISTS META.RLS_CONTINENT_ACCESS (
    ROLE_NAME      VARCHAR(100)  NOT NULL,
    CONTINENT_CODE VARCHAR(10)   NOT NULL,
    IS_ACTIVE      BOOLEAN       NOT NULL DEFAULT TRUE,
    CONSTRAINT PK_RLS_CONTINENT_ACCESS PRIMARY KEY (ROLE_NAME, CONTINENT_CODE)
);

-- MERGE, not INSERT: redeploying must not duplicate, and must not silently
-- re-activate an entry someone revoked by hand.
MERGE INTO META.RLS_CONTINENT_ACCESS t
USING (
              SELECT 'ACCOUNTADMIN'    AS ROLE_NAME, '*'   AS CONTINENT_CODE
    UNION ALL SELECT 'DWH_ADMIN',       '*'
    UNION ALL SELECT 'DWH_ANALYST_EU',  'EU'
    UNION ALL SELECT 'DWH_ANALYST_NAM', 'NAM'
) s
ON t.ROLE_NAME = s.ROLE_NAME AND t.CONTINENT_CODE = s.CONTINENT_CODE
WHEN NOT MATCHED THEN INSERT (ROLE_NAME, CONTINENT_CODE) VALUES (s.ROLE_NAME, s.CONTINENT_CODE);

-- The policy that reads it. Kept next to its mapping table rather than next to
-- the view it guards, because the pair is the security rule and the view is
-- just one place the rule is applied.
--
-- The body runs with the policy owner's rights, so the analyst roles need no
-- grant at all on META — they cannot read the table that decides what they can
-- read. CURRENT_ROLE() is the caller's role, which is what makes one policy
-- give three different answers.
--
-- IF NOT EXISTS rather than OR REPLACE: Snowflake refuses to replace a policy
-- that is attached to anything, so a redeploy would fail on the second run.
-- Change the rule with ALTER ROW ACCESS POLICY META.RAP_CONTINENT SET BODY -> ...
CREATE ROW ACCESS POLICY IF NOT EXISTS META.RAP_CONTINENT
    AS (continent_code VARCHAR) RETURNS BOOLEAN ->
        EXISTS (
            SELECT 1 FROM META.RLS_CONTINENT_ACCESS m
            WHERE m.ROLE_NAME = CURRENT_ROLE()
              AND m.IS_ACTIVE
              AND (m.CONTINENT_CODE = '*' OR m.CONTINENT_CODE = continent_code)
        )
    COMMENT = 'Restricts a row to roles granted its continent in META.RLS_CONTINENT_ACCESS.';
