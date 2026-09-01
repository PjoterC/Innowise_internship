-- Remove everything this project created. Not part of any pipeline — run it by
-- hand when you want a clean slate (or when redeploying after a DDL change,
-- since the CREATE ... IF NOT EXISTS statements will not alter existing tables).
DROP DATABASE IF EXISTS AIRLINE_DWH;
DROP ROLE IF EXISTS DWH_ADMIN;
DROP ROLE IF EXISTS DWH_ANALYST_EU;
DROP ROLE IF EXISTS DWH_ANALYST_NAM;
