-- The database-clone plan builder, as SQL.
--
--   cd Innowise_Snowflake
--   ../.venv/bin/python scripts/run_sql.py streamlit/sp_clone_database.sql streamlit/deploy.sql
--
-- Everything the Streamlit app knows about grants lives here: the app collects
-- form values, binds them as parameters, and renders whatever comes back. That
-- keeps the grant matrix callable from anywhere — a worksheet, an Airflow task,
-- another app — and keeps one implementation of it rather than two.
--
-- The procedure builds its plan first and executes it second, so DRY_RUN => TRUE
-- returns exactly the statements a real run would issue, without issuing any.

CREATE DATABASE IF NOT EXISTS TOOLING
    COMMENT = 'Operational tooling that is not part of a data pipeline.';

CREATE SCHEMA IF NOT EXISTS TOOLING.APPS
    COMMENT = 'Streamlit apps and the procedures behind them.';

-- The whole identifier check, in one place. Returns the upper-cased name, or
-- NULL when it is not a plain unquoted Snowflake identifier — every caller
-- below treats NULL as "reject the request", so a name that reaches an EXECUTE
-- IMMEDIATE has been through this function. A whitelist rather than quoting: a
-- form field that reaches `CREATE DATABASE <text>` is the entire attack surface
-- of the app, and this is the check that closes it.
CREATE OR REPLACE FUNCTION TOOLING.APPS.F_IDENTIFIER(NAME_IN VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Validate one unquoted Snowflake identifier; NULL when it is not one.'
AS
$$
    IFF(RLIKE(TRIM(NAME_IN, ' "'), '^[A-Za-z_][A-Za-z0-9_$]{0,254}$'),
        UPPER(TRIM(NAME_IN, ' "')),
        NULL)
$$;


CREATE OR REPLACE PROCEDURE TOOLING.APPS.SP_CLONE_DATABASE(
    SOURCE_DB            VARCHAR,   -- database to clone
    TARGET_DB            VARCHAR,   -- database to create
    OWNER_ROLE           VARCHAR,   -- role the clone is handed to
    OWNER_MODE           VARCHAR,   -- 'DEVELOPMENT' (writable) | 'READ_ONLY' (frozen)
    READONLY_ROLES       VARCHAR,   -- free text: one role per line, or comma-separated
    WAREHOUSE_NAME       VARCHAR,   -- '' to grant no warehouse
    CREATE_MISSING_ROLES BOOLEAN,
    TRANSFER_OWNERSHIP   BOOLEAN,
    REPLACE_EXISTING     BOOLEAN,   -- CREATE OR REPLACE DATABASE — destructive
    CONTINUE_ON_ERROR    BOOLEAN,   -- keep going when a single grant fails
    CLONE_COMMENT        VARCHAR,
    DRY_RUN              BOOLEAN    -- build the plan, execute nothing
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Clone a database and grant it to one owner role plus any number of read-only roles.'
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Object types a reader needs SELECT on. Snowflake has no "ALL OBJECTS"
    -- grant, so every type is its own GRANT — and its own FUTURE GRANT, so that
    -- a table created in the clone tomorrow is covered too.
    readable_types ARRAY DEFAULT ARRAY_CONSTRUCT(
        'TABLES', 'VIEWS', 'MATERIALIZED VIEWS', 'EXTERNAL TABLES',
        'DYNAMIC TABLES', 'ICEBERG TABLES', 'STREAMS');
    -- USAGE, not SELECT — a view that calls a UDF is unusable without it. A
    -- UDF cannot perform DML, which is what makes that grant safe to hand a
    -- reader.
    --
    -- PROCEDURES and SEQUENCES are deliberately absent. Both read as harmless
    -- companions to the list above, and both end the read-only guarantee:
    --
    --   PROCEDURES  a procedure defaults to EXECUTE AS OWNER, and a clone's
    --               objects are owned by whoever ran the CLONE — this app's
    --               role, since READ_ONLY leaves TRANSFER_OWNERSHIP off. USAGE
    --               would let a reader CALL a cloned loader and have it MERGE
    --               into the clone with the app's privileges rather than its
    --               own. AIRLINE_DWH ships six such procedures.
    --   SEQUENCES   USAGE is the privilege that permits NEXTVAL, which mutates
    --               the sequence. No read path needs it: a sequence is only
    --               reached through a column default, on INSERT.
    usable_types ARRAY DEFAULT ARRAY_CONSTRUCT(
        'FUNCTIONS', 'FILE FORMATS');
    -- What a development owner gets ALL PRIVILEGES on. ALL PRIVILEGES never
    -- includes OWNERSHIP, so this stays additive.
    --
    -- The DEVELOPMENT branch runs *instead of* the read-only one, not after
    -- it, so a type missing here is one the sandbox owner holds no privilege
    -- on at all — not even the SELECT a mere reader would have been given.
    -- That asymmetry is why ICEBERG TABLES belongs in this list and not only
    -- in readable_types.
    writable_types ARRAY DEFAULT ARRAY_CONSTRUCT(
        'TABLES', 'VIEWS', 'MATERIALIZED VIEWS', 'DYNAMIC TABLES',
        'EXTERNAL TABLES', 'ICEBERG TABLES', 'STAGES', 'FILE FORMATS',
        'SEQUENCES', 'STREAMS', 'TASKS', 'FUNCTIONS', 'PROCEDURES');
    -- The same list: whatever the sandbox owner may write to, it may also be
    -- handed outright when TRANSFER_OWNERSHIP is on.
    ownable_types ARRAY DEFAULT ARRAY_CONSTRUCT(
        'TABLES', 'VIEWS', 'MATERIALIZED VIEWS', 'DYNAMIC TABLES',
        'EXTERNAL TABLES', 'ICEBERG TABLES', 'STAGES', 'FILE FORMATS',
        'SEQUENCES', 'STREAMS', 'TASKS', 'FUNCTIONS', 'PROCEDURES');

    src        VARCHAR;
    tgt        VARCHAR;
    owner      VARCHAR;
    wh         VARCHAR DEFAULT '';
    owner_kind VARCHAR;
    problem    VARCHAR DEFAULT '';

    readers    ARRAY DEFAULT ARRAY_CONSTRUCT();  -- read-only roles, deduplicated
    ro_targets ARRAY DEFAULT ARRAY_CONSTRUCT();  -- readers, plus the owner in READ_ONLY mode
    granted    ARRAY DEFAULT ARRAY_CONSTRUCT();  -- everyone who needs the warehouse
    tokens     ARRAY;
    plan       ARRAY DEFAULT ARRAY_CONSTRUCT();
    steps      ARRAY DEFAULT ARRAY_CONSTRUCT();

    token      VARCHAR;
    role_name  VARCHAR;
    r          VARCHAR;
    obj        VARCHAR;
    item       VARIANT;
    stmt       VARCHAR;
    step_label VARCHAR;
    is_critical BOOLEAN;
    n_outer    INTEGER;
    n_inner    INTEGER;
    failures   INTEGER DEFAULT 0;
    stopped    BOOLEAN DEFAULT FALSE;
    quote      VARCHAR DEFAULT CHAR(39);   -- a single quote, unescaped by hand
BEGIN
    ------------------------------------------------------------------ validate
    -- Every name is checked before a single character of it is concatenated
    -- into a statement. Problems are collected rather than raised one at a time,
    -- so the form can show all of them at once.
    src   := TOOLING.APPS.F_IDENTIFIER(SOURCE_DB);
    tgt   := TOOLING.APPS.F_IDENTIFIER(TARGET_DB);
    owner := TOOLING.APPS.F_IDENTIFIER(OWNER_ROLE);
    owner_kind := UPPER(NVL(OWNER_MODE, ''));

    IF (src IS NULL) THEN
        problem := problem || 'Source database is missing or not a plain identifier. ';
    END IF;
    IF (tgt IS NULL) THEN
        problem := problem || 'Target database is missing or not a plain identifier. ';
    END IF;
    IF (owner IS NULL) THEN
        problem := problem || 'Owner role is missing or not a plain identifier. ';
    END IF;
    IF (src IS NOT NULL AND src = tgt) THEN
        problem := problem || 'Target database must differ from the source. ';
    END IF;
    IF (owner_kind NOT IN ('DEVELOPMENT', 'READ_ONLY')) THEN
        problem := problem || 'Owner mode must be DEVELOPMENT or READ_ONLY. ';
    END IF;

    IF (LENGTH(TRIM(NVL(WAREHOUSE_NAME, ''))) > 0) THEN
        wh := TOOLING.APPS.F_IDENTIFIER(WAREHOUSE_NAME);
        IF (wh IS NULL) THEN
            problem := problem || 'Warehouse name is not a plain identifier. ';
        END IF;
    END IF;

    -- One role per line or comma-separated, in the order typed.
    tokens := SPLIT(REPLACE(REPLACE(NVL(READONLY_ROLES, ''), CHAR(13), ''), CHAR(10), ','), ',');
    IF (ARRAY_SIZE(tokens) > 0) THEN
        n_outer := ARRAY_SIZE(tokens) - 1;
        FOR i IN 0 TO n_outer DO
            token := TRIM(GET(tokens, i)::VARCHAR);
            IF (LENGTH(token) > 0) THEN
                role_name := TOOLING.APPS.F_IDENTIFIER(token);
                IF (role_name IS NULL) THEN
                    problem := problem || 'Read-only role "' || token || '" is not a plain identifier. ';
                -- The owner is granted separately; listing it here as well would
                -- double every grant it gets.
                ELSEIF (role_name <> NVL(owner, '') AND NOT ARRAY_CONTAINS(role_name::VARIANT, readers)) THEN
                    readers := ARRAY_APPEND(readers, role_name);
                END IF;
            END IF;
        END FOR;
    END IF;

    IF (LENGTH(problem) > 0) THEN
        RETURN OBJECT_CONSTRUCT('status', 'invalid', 'detail', TRIM(problem),
                                'failed', 0, 'steps', ARRAY_CONSTRUCT());
    END IF;

    --------------------------------------------------------------- build the plan
    -- `critical` marks the statements the rest of the plan depends on. Those
    -- stop the run whatever CONTINUE_ON_ERROR says: granting on a database that
    -- was never created only produces a second, more confusing error.
    IF (CREATE_MISSING_ROLES) THEN
        granted := ARRAY_PREPEND(readers, owner::VARIANT);
        n_outer := ARRAY_SIZE(granted) - 1;
        FOR i IN 0 TO n_outer DO
            r := GET(granted, i)::VARCHAR;
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', 'Create role ' || r || ' if missing',
                'statement', 'CREATE ROLE IF NOT EXISTS ' || r,
                'critical', TRUE));
        END FOR;
    END IF;

    plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
        'step', 'Clone ' || src || ' -> ' || tgt,
        'statement', IFF(REPLACE_EXISTING, 'CREATE OR REPLACE DATABASE ', 'CREATE DATABASE ')
                     || tgt || ' CLONE ' || src,
        'critical', TRUE));

    -- The one free-text value in the whole procedure, and so the one that is
    -- escaped rather than whitelisted: a comment should keep whatever the caller
    -- typed. Both ways out of a string literal are doubled, backslash first.

    IF (LENGTH(TRIM(NVL(CLONE_COMMENT, ''))) > 0) THEN
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', 'Comment on ' || tgt,
            'statement', 'ALTER DATABASE ' || tgt || ' SET COMMENT = ' || quote
                         || REPLACE(REPLACE(CLONE_COMMENT, '\\', '\\\\'), quote, quote || quote)
                         || quote,
            'critical', FALSE));
    END IF;

    -- Warehouse USAGE is not part of the clone, and without it every one of
    -- these roles can list objects but not run a query — which reads as a bug
    -- rather than as a missing grant.
    IF (wh IS NOT NULL AND LENGTH(wh) > 0) THEN
        granted := ARRAY_PREPEND(readers, owner::VARIANT);
        n_outer := ARRAY_SIZE(granted) - 1;
        FOR i IN 0 TO n_outer DO
            r := GET(granted, i)::VARCHAR;
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', r || ': USAGE on warehouse ' || wh,
                'statement', 'GRANT USAGE ON WAREHOUSE ' || wh || ' TO ROLE ' || r,
                'critical', FALSE));
        END FOR;
    END IF;

    -- Read-only grants: the listed roles, and the owner too when the clone is a
    -- frozen snapshot. Readers come before the owner, and ownership comes after
    -- everything, so COPY CURRENT GRANTS has something to copy.
    ro_targets := readers;
    IF (owner_kind = 'READ_ONLY') THEN
        ro_targets := ARRAY_APPEND(ro_targets, owner::VARIANT);
    END IF;

    IF (ARRAY_SIZE(ro_targets) > 0) THEN
        n_outer := ARRAY_SIZE(ro_targets) - 1;
        FOR i IN 0 TO n_outer DO
            r := GET(ro_targets, i)::VARCHAR;
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', r || ': USAGE on ' || tgt,
                'statement', 'GRANT USAGE ON DATABASE ' || tgt || ' TO ROLE ' || r,
                'critical', FALSE));
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', r || ': USAGE on schemas',
                'statement', 'GRANT USAGE ON ALL SCHEMAS IN DATABASE ' || tgt || ' TO ROLE ' || r,
                'critical', FALSE));
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', r || ': USAGE on future schemas',
                'statement', 'GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ' || tgt || ' TO ROLE ' || r,
                'critical', FALSE));

            n_inner := ARRAY_SIZE(readable_types) - 1;
            FOR j IN 0 TO n_inner DO
                obj := GET(readable_types, j)::VARCHAR;
                plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                    'step', r || ': SELECT on ' || LOWER(obj),
                    'statement', 'GRANT SELECT ON ALL ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || r,
                    'critical', FALSE));
                plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                    'step', r || ': SELECT on future ' || LOWER(obj),
                    'statement', 'GRANT SELECT ON FUTURE ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || r,
                    'critical', FALSE));
            END FOR;

            n_inner := ARRAY_SIZE(usable_types) - 1;
            FOR j IN 0 TO n_inner DO
                obj := GET(usable_types, j)::VARCHAR;
                plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                    'step', r || ': USAGE on ' || LOWER(obj),
                    'statement', 'GRANT USAGE ON ALL ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || r,
                    'critical', FALSE));
                plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                    'step', r || ': USAGE on future ' || LOWER(obj),
                    'statement', 'GRANT USAGE ON FUTURE ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || r,
                    'critical', FALSE));
            END FOR;
        END FOR;
    END IF;

    -- A development owner gets a sandbox: new schemas of its own, and full DML
    -- on everything the clone brought with it.
    IF (owner_kind = 'DEVELOPMENT') THEN
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', owner || ': USAGE, MONITOR, CREATE SCHEMA on ' || tgt,
            'statement', 'GRANT USAGE, MONITOR, CREATE SCHEMA ON DATABASE ' || tgt || ' TO ROLE ' || owner,
            'critical', FALSE));
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', owner || ': ALL on schemas',
            'statement', 'GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ' || tgt || ' TO ROLE ' || owner,
            'critical', FALSE));
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', owner || ': ALL on future schemas',
            'statement', 'GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE ' || tgt || ' TO ROLE ' || owner,
            'critical', FALSE));

        n_inner := ARRAY_SIZE(writable_types) - 1;
        FOR j IN 0 TO n_inner DO
            obj := GET(writable_types, j)::VARCHAR;
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', owner || ': ALL on ' || LOWER(obj),
                'statement', 'GRANT ALL PRIVILEGES ON ALL ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || owner,
                'critical', FALSE));
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', owner || ': ALL on future ' || LOWER(obj),
                'statement', 'GRANT ALL PRIVILEGES ON FUTURE ' || obj || ' IN DATABASE ' || tgt || ' TO ROLE ' || owner,
                'critical', FALSE));
        END FOR;
    END IF;

    -- Hand the clone over, children first. COPY CURRENT GRANTS keeps the
    -- read-only grants made above — the default, REVOKE CURRENT GRANTS, would
    -- quietly undo them. The database itself goes last: while the running role
    -- still owns it, it holds the USAGE needed to name the objects inside.
    IF (TRANSFER_OWNERSHIP) THEN
        n_inner := ARRAY_SIZE(ownable_types) - 1;
        FOR j IN 0 TO n_inner DO
            obj := GET(ownable_types, j)::VARCHAR;
            plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
                'step', owner || ': OWNERSHIP of ' || LOWER(obj),
                'statement', 'GRANT OWNERSHIP ON ALL ' || obj || ' IN DATABASE ' || tgt
                             || ' TO ROLE ' || owner || ' COPY CURRENT GRANTS',
                'critical', FALSE));
        END FOR;
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', owner || ': OWNERSHIP of schemas',
            'statement', 'GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE ' || tgt
                         || ' TO ROLE ' || owner || ' COPY CURRENT GRANTS',
            'critical', FALSE));
        plan := ARRAY_APPEND(plan, OBJECT_CONSTRUCT(
            'step', owner || ': OWNERSHIP of ' || tgt,
            'statement', 'GRANT OWNERSHIP ON DATABASE ' || tgt
                         || ' TO ROLE ' || owner || ' COPY CURRENT GRANTS',
            'critical', FALSE));
    END IF;

    ------------------------------------------------------------------- run it
    n_outer := ARRAY_SIZE(plan) - 1;
    FOR i IN 0 TO n_outer DO
        item        := GET(plan, i);
        step_label  := GET(item, 'step')::VARCHAR;
        stmt        := GET(item, 'statement')::VARCHAR;
        is_critical := GET(item, 'critical')::BOOLEAN;

        IF (DRY_RUN) THEN
            steps := ARRAY_APPEND(steps, OBJECT_CONSTRUCT(
                'step_no', i + 1, 'step', step_label, 'statement', stmt,
                'status', 'planned', 'detail', ''));
        ELSE
            BEGIN
                EXECUTE IMMEDIATE :stmt;
                steps := ARRAY_APPEND(steps, OBJECT_CONSTRUCT(
                    'step_no', i + 1, 'step', step_label, 'statement', stmt,
                    'status', 'ok', 'detail', ''));
            EXCEPTION
                WHEN OTHER THEN
                    failures := failures + 1;
                    steps := ARRAY_APPEND(steps, OBJECT_CONSTRUCT(
                        'step_no', i + 1, 'step', step_label, 'statement', stmt,
                        'status', 'failed', 'detail', SQLERRM));
                    IF (is_critical OR NOT CONTINUE_ON_ERROR) THEN
                        stopped := TRUE;
                    END IF;
            END;

            IF (stopped) THEN
                steps := ARRAY_APPEND(steps, OBJECT_CONSTRUCT(
                    'step_no', i + 2, 'step', 'Run stopped', 'statement', '',
                    'status', 'skipped',
                    'detail', (ARRAY_SIZE(plan) - i - 1) || ' statements not attempted.'));
                BREAK;
            END IF;
        END IF;
    END FOR;

    RETURN OBJECT_CONSTRUCT(
        'status', IFF(DRY_RUN, 'planned', IFF(failures > 0, 'failed', 'ok')),
        'detail', '',
        'failed', failures,
        'steps', steps);
END;
$$;

