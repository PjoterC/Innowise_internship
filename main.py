import data_loader
from db_connector import DatabaseConnection, PostgresConnection
from unloader import JsonConverter, XMLConverter, ReadQueryWrapper


def LoadData(connection) -> None:
    loader = data_loader.DataLoader(connection)
  
    roomcount = loader.Load(data_loader.JsonFileDataSource("rooms.json"), data_loader.ROOMS)
    print(f"Loaded {roomcount} rooms")
    studentcount = loader.Load(data_loader.JsonFileDataSource("students.json"), data_loader.STUDENTS)
    print(f"Loaded {studentcount} students")


def main(database: DatabaseConnection = None) -> None: # type: ignore
    database = database or PostgresConnection()
    try:
        connection = database.Connect()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    print("✅ Connection successful")
    try:
        LoadData(connection)
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        return

    try:
        qWrapper = ReadQueryWrapper(connection=connection)
        result1 = qWrapper.GetStudentPerRoom()
        result2 = qWrapper.GetFiveLowestAvgAgeRooms()
        result3 = qWrapper.GetFiveLargestAgeDifferenceRooms()
        result4 = qWrapper.GetRoomsWithDifferentSexes()

        finalJSON = JsonConverter().ConvertRecords(result4)
        #print(finalJSON)
        finalXML = XMLConverter().ConvertRecords(result4)
        print(finalXML)
    finally:
        database.Close()



if __name__ == "__main__":
    main()
