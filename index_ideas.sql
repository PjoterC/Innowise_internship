CREATE INDEX idx_students_room ON students(room);

DROP INDEX idx_students_room;

ANALYZE students;

-- for checking index efficiency set to off (on small tables sequential scan is faster)
SET enable_seqscan = on;
