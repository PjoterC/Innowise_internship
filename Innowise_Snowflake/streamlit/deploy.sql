-- Deploy the database-clone app — the procedure behind it and the form itself.
--
--   cd Innowise_Snowflake
--   ../.venv/bin/python scripts/run_sql.py streamlit/deploy.sql
--
-- This file is self-contained on purpose. It used to ship only the form, on the
-- understanding that sp_clone_database.sql had been run first; anyone who ran
-- the file named "deploy" on its own got a working app in front of a procedure
-- that did not exist, and the app could only report it as
--
--   Unknown user-defined function TOOLING.APPS.SP_CLONE_DATABASE
--
-- so the procedure is now staged and executed below, before the app that calls
-- it is created. Passing sp_clone_database.sql to run_sql.py as well is still
-- harmless — every statement in it is CREATE OR REPLACE or IF NOT EXISTS.
--
-- Run through run_sql.py rather than a Snowsight worksheet because PUT is a
-- client-side driver command that reads a local file, which a worksheet cannot
-- do. The file:// paths below are relative to Innowise_Snowflake/.
--
-- The role running this ends up owning the app, and a Streamlit app in
-- Snowflake runs with its owner's rights — so the app can do whatever this role
-- can do, for everyone allowed to open it. See streamlit/README.md.

CREATE DATABASE IF NOT EXISTS TOOLING
    COMMENT = 'Operational tooling that is not part of a data pipeline.';

CREATE SCHEMA IF NOT EXISTS TOOLING.APPS
    COMMENT = 'Streamlit apps and the procedures behind them.';

CREATE STAGE IF NOT EXISTS TOOLING.APPS.STG_DB_CLONE_APP
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Source files for the DB_CLONE Streamlit app.';

-- AUTO_COMPRESS = FALSE because Streamlit reads these files as-is; OVERWRITE so
-- redeploying is just a re-run.
PUT file://streamlit/streamlit_app.py @TOOLING.APPS.STG_DB_CLONE_APP/db_clone AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
PUT file://streamlit/environment.yml  @TOOLING.APPS.STG_DB_CLONE_APP/db_clone AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- The procedure, staged under /sql rather than /db_clone: ROOT_LOCATION below
-- points at /db_clone, and everything under it is served as part of the app.
-- AUTO_COMPRESS = FALSE again, because EXECUTE IMMEDIATE FROM reads the file as
-- text and cannot read a .gz.
PUT file://streamlit/sp_clone_database.sql @TOOLING.APPS.STG_DB_CLONE_APP/sql AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- Run it from the stage. This is the whole reason the file is staged: a PUT
-- only uploads, and the procedure has to exist before the app that calls it.
EXECUTE IMMEDIATE FROM @TOOLING.APPS.STG_DB_CLONE_APP/sql/sp_clone_database.sql;

-- Prove it landed, and stop here if it did not. Without this the next statement
-- would happily create an app whose every action fails, which is exactly the
-- failure this file exists to prevent — and run_sql.py stops on the first error,
-- so raising here means no half-deployed app.
EXECUTE IMMEDIATE $$
DECLARE
    n INTEGER;
    E_NO_PROCEDURE EXCEPTION (-20020,
        'TOOLING.APPS.SP_CLONE_DATABASE was not created. Read the EXECUTE IMMEDIATE FROM error above; the app is not deployed.');
BEGIN
    SELECT COUNT(*) INTO :n
      FROM TOOLING.INFORMATION_SCHEMA.PROCEDURES
     WHERE PROCEDURE_SCHEMA = 'APPS' AND PROCEDURE_NAME = 'SP_CLONE_DATABASE';
    IF (n = 0) THEN
        RAISE E_NO_PROCEDURE;
    END IF;
    RETURN 'SP_CLONE_DATABASE is present';
END;
$$;

-- QUERY_WAREHOUSE is required and cannot be an expression, so the statement is
-- assembled around CURRENT_WAREHOUSE() instead of hard-coding one account's.
SET wh_name = CURRENT_WAREHOUSE();
SET create_app = 'CREATE OR REPLACE STREAMLIT TOOLING.APPS.DB_CLONE
    ROOT_LOCATION = ''@TOOLING.APPS.STG_DB_CLONE_APP/db_clone''
    MAIN_FILE = ''/streamlit_app.py''
    QUERY_WAREHOUSE = ' || $wh_name || '
    TITLE = ''Database clone''
    COMMENT = ''Clone a database and grant it to a development or read-only owner.''';
EXECUTE IMMEDIATE $create_app;

SHOW STREAMLITS IN SCHEMA TOOLING.APPS;

-- Privileges the caller needs, over and above what it has already. The
-- procedure is EXECUTE AS CALLER, so these are needed by whoever uses the app
-- or calls the procedure — in the app's case, the role that owns it. Uncomment
-- and set the role; account-level grants need ACCOUNTADMIN or SECURITYADMIN.
--
--   SET app_role = 'DWH_ADMIN';
--   GRANT CREATE DATABASE ON ACCOUNT TO ROLE IDENTIFIER($app_role);   -- to clone
--   GRANT CREATE ROLE     ON ACCOUNT TO ROLE IDENTIFIER($app_role);   -- "create missing roles"
--   GRANT MANAGE GRANTS   ON ACCOUNT TO ROLE IDENTIFIER($app_role);   -- "transfer OWNERSHIP"
--   GRANT USAGE ON DATABASE AIRLINE_DWH TO ROLE IDENTIFIER($app_role);  -- each source
--
-- Who may open the app and call the procedure behind it:
--
--   GRANT USAGE ON DATABASE  TOOLING               TO ROLE IDENTIFIER($app_role);
--   GRANT USAGE ON SCHEMA    TOOLING.APPS          TO ROLE IDENTIFIER($app_role);
--   GRANT USAGE ON STREAMLIT TOOLING.APPS.DB_CLONE TO ROLE IDENTIFIER($app_role);
--   GRANT USAGE ON FUNCTION  TOOLING.APPS.F_IDENTIFIER(VARCHAR) TO ROLE IDENTIFIER($app_role);
--   GRANT USAGE ON PROCEDURE TOOLING.APPS.SP_CLONE_DATABASE(
--       VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
--       BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, VARCHAR, BOOLEAN) TO ROLE IDENTIFIER($app_role);
