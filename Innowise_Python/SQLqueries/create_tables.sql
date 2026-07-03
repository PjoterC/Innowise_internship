-- A file for creating initial database tables. Runs automatically at the start of the program.

CREATE TABLE IF NOT EXISTS rooms (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    birthday TIMESTAMP,
    room INT,
    sex CHAR,
    FOREIGN KEY (room) REFERENCES rooms (id)
);
