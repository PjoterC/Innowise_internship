WITH student_counts as (
    SELECT room, COUNT(id) as student_count FROM students GROUP BY room
)

SELECT rooms.*, student_counts.student_count FROM rooms LEFT JOIN student_counts ON rooms.id = student_counts.room




SELECT rooms.*, AVG(age(CURRENT_DATE, students.birthday)) as avg_age FROM rooms LEFT JOIN students ON rooms.id = students.room
GROUP BY rooms.id
ORDER BY avg_age ASC
LIMIT 5;


