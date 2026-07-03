-- File used for running queries on the database and analysing performance.


-- for checking indexes set to off (on small tables sequential scan is faster)
SET enable_seqscan = off;

-- rooms with student count
EXPLAIN (ANALYZE, BUFFERS)
WITH student_counts AS (
        SELECT room, COUNT(id) AS student_count FROM students GROUP BY room
        )
SELECT rooms.*, COALESCE(student_counts.student_count, 0) AS student_count
FROM rooms
LEFT JOIN student_counts ON rooms.id = student_counts.room;



-- 5 rooms with the lowest average age of students
EXPLAIN (ANALYZE, BUFFERS)
SELECT
rooms.id,
rooms.name,
AVG(EXTRACT(YEAR FROM age(CURRENT_DATE, students.birthday)))::numeric(10, 2) AS avg_age_years
FROM rooms
INNER JOIN students ON rooms.id = students.room
GROUP BY rooms.id, rooms.name
ORDER BY avg_age_years ASC
LIMIT 5;





-- 5 rooms with the largest difference in the age of students
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
rooms.id, 
rooms.name, 
(EXTRACT(YEAR FROM age(MAX(students.birthday), MIN(students.birthday))))::integer AS age_diff_years
FROM rooms
JOIN students ON rooms.id = students.room
GROUP BY rooms.id, rooms.name
ORDER BY age_diff_years DESC
LIMIT 5;




-- Rooms where students of different sexes live
EXPLAIN (ANALYZE, BUFFERS)
SELECT rooms.*
FROM rooms
JOIN students ON rooms.id = students.room
GROUP BY rooms.id
HAVING COUNT(DISTINCT students.sex) > 1;




