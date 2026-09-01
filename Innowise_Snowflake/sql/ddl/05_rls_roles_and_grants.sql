-- Demo roles — the other half of row-level security. A filter is only a control
-- if the reader cannot go around it, so these roles get USAGE on MART and
-- SELECT on the secure view, and nothing at all on RAW, CORE or META. The view
-- still works for them because its owner can read the base tables (Snowflake's
-- ownership chain) while the caller cannot.
--
-- Needs ACCOUNTADMIN or SECURITYADMIN — this is the only script that touches
-- anything outside the AIRLINE_DWH database.

USE DATABASE AIRLINE_DWH;

-- Read from the session so nothing is hard-coded to one account.
SET wh_name      = CURRENT_WAREHOUSE();
SET grantee_user = CURRENT_USER();

CREATE ROLE IF NOT EXISTS DWH_ADMIN        COMMENT = 'Pipeline owner — every continent.';
CREATE ROLE IF NOT EXISTS DWH_ANALYST_EU   COMMENT = 'Europe desk.';
CREATE ROLE IF NOT EXISTS DWH_ANALYST_NAM  COMMENT = 'North America desk.';

-- Without warehouse USAGE the roles can see object names but not run a query,
-- which looks like a bug rather than a missing grant.
GRANT USAGE ON WAREHOUSE IDENTIFIER($wh_name) TO ROLE DWH_ADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER($wh_name) TO ROLE DWH_ANALYST_EU;
GRANT USAGE ON WAREHOUSE IDENTIFIER($wh_name) TO ROLE DWH_ANALYST_NAM;

GRANT USAGE ON DATABASE AIRLINE_DWH TO ROLE DWH_ADMIN;
GRANT USAGE ON DATABASE AIRLINE_DWH TO ROLE DWH_ANALYST_EU;
GRANT USAGE ON DATABASE AIRLINE_DWH TO ROLE DWH_ANALYST_NAM;

GRANT USAGE ON SCHEMA MART TO ROLE DWH_ADMIN;
GRANT USAGE ON SCHEMA MART TO ROLE DWH_ANALYST_EU;
GRANT USAGE ON SCHEMA MART TO ROLE DWH_ANALYST_NAM;

-- The secure view, and nothing else. Note the absence of GRANT SELECT ON ALL
-- TABLES anywhere in this file.
GRANT SELECT ON VIEW MART.V_FLIGHT_BOOKING_SECURE TO ROLE DWH_ADMIN;
GRANT SELECT ON VIEW MART.V_FLIGHT_BOOKING_SECURE TO ROLE DWH_ANALYST_EU;
GRANT SELECT ON VIEW MART.V_FLIGHT_BOOKING_SECURE TO ROLE DWH_ANALYST_NAM;

-- DWH_ADMIN operates the pipeline, so it reads its own audit trail.
GRANT USAGE  ON SCHEMA META               TO ROLE DWH_ADMIN;
GRANT SELECT ON TABLE  META.ETL_AUDIT_LOG TO ROLE DWH_ADMIN;

-- Hand all three to whoever ran this, so the demo can be driven with USE ROLE
-- from a single session.
GRANT ROLE DWH_ADMIN       TO USER IDENTIFIER($grantee_user);
GRANT ROLE DWH_ANALYST_EU  TO USER IDENTIFIER($grantee_user);
GRANT ROLE DWH_ANALYST_NAM TO USER IDENTIFIER($grantee_user);
