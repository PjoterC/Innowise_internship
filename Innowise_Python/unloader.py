
from table_handling import Record
from abc import ABC, abstractmethod
from typing import List
import json
import xml.etree.ElementTree as ET




class DataConverter(ABC):
    """
    An abstract class used to return the SQL record data obtained from the query wrapper in specific format.
    """

    @abstractmethod
    def ConvertRecords(self, records: List[Record]) -> str:
        """
        Converts records obtained by the data unloader to a specific format.
        """
        pass

    
class JsonConverter(DataConverter):
    """JSON"""
    def ConvertRecords(self, records: List[Record]) -> str:
        return json.dumps(records, indent=2, default=str)


class XMLConverter(DataConverter):
    """XML"""
    def ConvertRecords(self, records: List[Record]) -> str:
        root = ET.Element("records")
        for record in records:
            record_element = ET.SubElement(root, "record")
            for key, value in record.items():
                field = ET.SubElement(record_element, key)
                field.text = "" if value is None else str(f"\"{value}\"")
        ET.indent(root)
        return ET.tostring(root, encoding="unicode")

    



    
class ReadQueryWrapper:
    """
    Wrapper for read-only queries.
    """
    def __init__(self, connection) -> None:
        self._connection = connection

    def _fetch(self, sql: str) -> List[Record]:
        cursor = self._connection.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    

    # 4 requested SQL query wrappers (postgres compatible)
    def GetStudentPerRoom(self) -> List[Record]:
        """
        List of rooms and the number of students in each of them.
        """
        return self._fetch(
        '''
        WITH student_counts AS (
        SELECT room, COUNT(id) AS student_count FROM students GROUP BY room
        )
        SELECT rooms.*, COALESCE(student_counts.student_count, 0) AS student_count
        FROM rooms
        LEFT JOIN student_counts ON rooms.id = student_counts.room;


        '''
        )

    def GetFiveLowestAvgAgeRooms(self) -> List[Record]:
        """
        5 rooms with the smallest average age of students
        """
        return self._fetch(
        '''
        SELECT rooms.*, AVG(age(CURRENT_DATE, students.birthday)) as avg_age FROM rooms INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.id
        ORDER BY avg_age ASC
        LIMIT 5;

        '''
        )
    def GetFiveLargestAgeDifferenceRooms(self) -> List[Record]:
        """
        5 rooms with the largest difference in the age of students
        """
        return self._fetch(
        '''
        SELECT rooms.*, (MAX(students.birthday) - MIN(students.birthday)) AS age_diff
        FROM rooms
        JOIN students ON rooms.id = students.room
        GROUP BY rooms.id
        ORDER BY age_diff DESC
        LIMIT 5;

        '''

        )

    def GetRoomsWithDifferentSexes(self) -> List[Record]:
        """
        List of rooms where different-sex students live
        """
        return self._fetch(
        '''
        SELECT rooms.*
        FROM rooms
        JOIN students ON rooms.id = students.room
        GROUP BY rooms.id
        HAVING COUNT(DISTINCT students.sex) > 1;
        '''
    )