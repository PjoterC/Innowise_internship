from pathlib import Path
import json


class DataLoader:
    def __init__(self, dbConnection):
        self.dbConnection = dbConnection
    
    def LoadRooms(self, jsonPath):
        """
        Load rooms data from a JSON file into the database.
        """
        try:
            with open(jsonPath, 'r') as file:
                roomsData = json.load(file)
            
            cursor = self.dbConnection.cursor()
            for room in roomsData:
                cursor.execute(
                    "INSERT INTO rooms (id, name) VALUES (%s, %s)",
                    (room['id'], room['name'])
                )
            self.dbConnection.commit()
            print("✅ Rooms data loaded successfully")
        except Exception as e:
            print(f"Error loading rooms data: {e}")

    def LoadStudents(self, jsonPath):
        """
        Load students data from a JSON file into the database.
        """
        try:
            with open(jsonPath, 'r') as file:
                studentsData = json.load(file)
            
            cursor = self.dbConnection.cursor()
            for student in studentsData:
                cursor.execute(
                    "INSERT INTO students (id, name, birthday, room, sex) VALUES (%s, %s, %s, %s, %s)",
                    (student['id'], student['name'], student['birthday'], student['room'], student['sex'])
                )
            self.dbConnection.commit()
            print("✅ Students data loaded successfully")
        except Exception as e:
            print(f"Error loading students data: {e}")


   