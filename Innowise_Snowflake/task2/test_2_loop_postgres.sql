-- =============================================================================
-- Task 2 - PostgreSQL solution
--
-- Question: can the 20 hand-written CALL statements be replaced by a loop?
-- Answer:   yes, but only two of the three arguments are data.
--
--   arg1 (batch date)  - data, must be listed
--   arg2 (loaded_at)   - data, must be listed; no formula relates it to arg1
--   arg3 (prev batch)  - DERIVED: it is always arg1 of the preceding call,
--                        i.e. LAG(arg1) over the calls ordered by arg1.
--                        The very first call has no predecessor, so it needs a
--                        seed value ('2025-01-28').
--
-- So the loop does not shrink the payload to nothing - it removes the third
-- column and, more importantly, removes the chance of mis-typing the chain.
-- =============================================================================

CREATE OR REPLACE PROCEDURE run_test_2_batches(
    p_seed_prev date DEFAULT DATE '2025-01-28'
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT batch_date,
               loaded_at,
               -- The three-argument form of LAG supplies the seed for the
               -- first row, so no special case is needed inside the loop.
               LAG(batch_date, 1, p_seed_prev) OVER (ORDER BY batch_date)
                   AS prev_batch_date
        FROM (VALUES
            (DATE '2025-02-06', TIMESTAMP '2025-02-12 09:38:25.999982'),
            (DATE '2025-02-14', TIMESTAMP '2025-02-14 16:17:14.095384'),
            (DATE '2025-02-20', TIMESTAMP '2025-02-21 08:41:53.643244'),
            (DATE '2025-02-25', TIMESTAMP '2025-03-11 15:52:28.575590'),
            (DATE '2025-03-06', TIMESTAMP '2025-03-13 15:35:21.729785'),
            (DATE '2025-03-13', TIMESTAMP '2025-03-13 16:32:27.178218'),
            (DATE '2025-03-20', TIMESTAMP '2025-03-26 08:35:19.585812'),
            (DATE '2025-03-27', TIMESTAMP '2025-03-28 07:23:03.611707'),
            (DATE '2025-04-07', TIMESTAMP '2025-04-08 18:57:03.804270'),
            (DATE '2025-04-10', TIMESTAMP '2025-04-15 11:19:51.275211'),
            (DATE '2025-04-14', TIMESTAMP '2025-04-15 14:34:32.097939'),
            (DATE '2025-04-24', TIMESTAMP '2025-04-24 14:41:48.705573'),
            (DATE '2025-05-02', TIMESTAMP '2025-05-08 11:05:44.640510'),
            (DATE '2025-05-15', TIMESTAMP '2025-05-21 10:00:08.361011'),
            (DATE '2025-05-22', TIMESTAMP '2025-05-28 08:07:06.096731'),
            (DATE '2025-05-29', TIMESTAMP '2025-05-30 10:01:45.906511'),
            (DATE '2025-06-05', TIMESTAMP '2025-06-09 09:22:04.668390'),
            (DATE '2025-06-19', TIMESTAMP '2025-07-03 08:27:40.115104'),
            (DATE '2025-06-26', TIMESTAMP '2025-07-03 09:15:38.292950'),
            (DATE '2025-07-03', TIMESTAMP '2025-07-07 10:53:30.915895')
        ) AS v(batch_date, loaded_at)
        ORDER BY batch_date
    LOOP
        -- The original script passed untyped string literals, so the arguments
        -- are rebuilt as text in exactly the original spelling. That keeps this
        -- working whether test_2 is declared as (text, text, text) or with
        -- date/timestamp parameters, since an untyped-looking text value still
        -- has to be cast explicitly for the latter - see the note below.
        --
        -- Postgres stores microseconds, not nanoseconds, so the trailing '000'
        -- is re-appended. Every input ended in '000', so nothing is lost.
        CALL test_2(
            to_char(r.batch_date,       'YYYY-MM-DD'),
            to_char(r.loaded_at,        'YYYY-MM-DD HH24:MI:SS.US') || '000',
            to_char(r.prev_batch_date,  'YYYY-MM-DD')
        );

        -- If test_2 is declared as (date, timestamp, date), delete the CALL
        -- above and use this one instead:
        -- CALL test_2(r.batch_date, r.loaded_at, r.prev_batch_date);
    END LOOP;
END;
$procedure$;


-- Run it:
CALL run_test_2_batches();

-- Or with a different seed for the first call's third argument:
-- CALL run_test_2_batches(DATE '2025-01-28');


-- -----------------------------------------------------------------------------
-- Note on transactions
--
-- The 20 loose CALLs each committed on their own. Here they run inside one
-- procedure, so an error on call #5 rolls back calls #1-#4 as well, unless
-- test_2 issues its own COMMIT. If per-call durability matters, add a COMMIT
-- inside the loop - a PL/pgSQL *procedure* is allowed to do that (a function
-- is not).
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- Variant: keep the driver in a table instead of an inline VALUES list, so
-- adding a batch is an INSERT rather than an edit to the procedure body.
--
--   CREATE TABLE test_2_batches (
--       batch_date date PRIMARY KEY,
--       loaded_at  timestamp NOT NULL
--   );
--
-- and replace the derived table in the FOR loop with:
--
--   FROM test_2_batches
-- -----------------------------------------------------------------------------
