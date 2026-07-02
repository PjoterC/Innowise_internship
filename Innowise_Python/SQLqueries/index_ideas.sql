-- EXPLANATION OF INDEX IDEAS


-- Index on students table for room and birthday columns to speed up queries filtering by room and birthday
-- All requested queries join on room so indexing on room is an obvious choice.
-- Indexing on birthday is useful for the queries that calculate the average age and the age difference of students in a room.
-- For the queries that use the birthday column, the index was confirmed to be selected over other options by the EXPLAIN (ANALYZE, BUFFERS) command.
-- NOTE: In order to test the index selection, the enable_seqscan option was set to off, as on small tables sequential scan is faster than index scan.
CREATE INDEX idx_students_room_birthday ON students(room, birthday);




-- Index on students table for room and sex columns to speed up queries filtering by room and sex
-- All requested queries join on room so indexing on room is an obvious choice.
-- Indexing on sex is useful for the query that checks for rooms where students of different sexes live
-- For the query that uses the sex column, the index was confirmed to be selected over other options by the EXPLAIN (ANALYZE, BUFFERS) command.
-- For the first query that only counts the number of students in each room this index was preferred over the birthday index due to being slightly smaller in size and thus more efficient for the query.
-- NOTE: In order to test the index selection, the enable_seqscan option was set to off, as on small tables sequential scan is faster than index scan.
CREATE INDEX idx_students_room_sex ON students(room, sex);


--For testing
--DROP INDEX idx_students_room_birthday;
--DROP INDEX idx_students_room_sex;

