-- Cheapest possible proof that the connection works: every one of these is
-- answered by the session itself, so it needs no warehouse and burns no credits.
SELECT CURRENT_VERSION()   AS version,
       CURRENT_ACCOUNT()   AS account,
       CURRENT_USER()      AS user,
       CURRENT_ROLE()      AS role,
       CURRENT_WAREHOUSE() AS warehouse,
       CURRENT_DATABASE()  AS database,
       CURRENT_SCHEMA()    AS schema;
