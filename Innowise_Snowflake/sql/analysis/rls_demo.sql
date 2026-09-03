-- Proof that row-level security works. Run the whole file, top to bottom, in
-- one session. Every result prints ACTING_ROLE, because a result you cannot
-- attribute to a role proves nothing.
--
-- Requires 05_rls_roles_and_grants.sql to have been run first.


-- RUN FROM SNOWSIGHT NOT LOCALLY - PAT does not allow USE ROLE

USE DATABASE AIRLINE_DWH;

-- Where the policy is in force, and on which column.
SELECT POLICY_NAME, REF_ENTITY_NAME, REF_ARG_COLUMN_NAMES, POLICY_STATUS
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME => 'META.RAP_CONTINENT'));

-- --- Positive tests: one query, three roles, three answers -----------------
-- These are only meaningful once the pipeline has loaded data. On an empty
-- warehouse all three return nothing, which demonstrates neither filtering nor
-- its absence.

USE ROLE DWH_ANALYST_EU;
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() AS acting_role, CONTINENT_CODE, COUNT(*) AS bookings
FROM MART.V_FLIGHT_BOOKING_SECURE GROUP BY 1, 2 ORDER BY 2;

USE ROLE DWH_ANALYST_NAM;
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() AS acting_role, CONTINENT_CODE, COUNT(*) AS bookings
FROM MART.V_FLIGHT_BOOKING_SECURE GROUP BY 1, 2 ORDER BY 2;

USE ROLE DWH_ADMIN;
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() AS acting_role, CONTINENT_CODE, COUNT(*) AS bookings
FROM MART.V_FLIGHT_BOOKING_SECURE GROUP BY 1, 2 ORDER BY 2;

-- --- Negative test: THIS STATEMENT IS EXPECTED TO FAIL ----------------------
-- The filter is only a control if the analyst cannot go around it. The analyst
-- roles hold no grant on CORE at all, so this must not run.
--
--   PASS  ->  SQL compilation error:
--             Object 'CORE.FCT_FLIGHT_BOOKING' does not exist or not authorized.
--   FAIL  ->  it returns a number. Even 0 is a failure: 0 means the table was
--             readable and happened to be empty. If it returns a count while
--             ACTING_ROLE says DWH_ANALYST_EU, the privilege came from a
--             SECONDARY role -- check the SECONDARY column in the same result,
--             and see the note on secondary roles at the top of this file.
--

USE ROLE DWH_ANALYST_EU;
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() AS acting_role, CURRENT_SECONDARY_ROLES() AS secondary,
       COUNT(*) AS this_should_not_be_reachable
FROM CORE.FCT_FLIGHT_BOOKING;
