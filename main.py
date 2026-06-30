import data_loader
from db_connector import DatabaseConnection, PostgresConnection
from unloader import JsonConverter, XMLConverter, ReadQueryWrapper
from table_handling import TableCreator


def LoadData(connection) -> None:
    loader = data_loader.DataLoader(connection)
  
    roomcount = loader.Load(data_loader.JsonFileDataSource("rooms.json"), data_loader.ROOMS)
    print(f"Loaded {roomcount} rooms")
    studentcount = loader.Load(data_loader.JsonFileDataSource("students.json"), data_loader.STUDENTS)
    print(f"Loaded {studentcount} students")


def PromptChoice(prompt: str, choices: list[int]) -> int:
    """Prompt until the user enters one of the allowed integer choices."""
    while True:
        var = input(prompt).strip()
        if var.isdigit() and int(var) in choices:
            return int(var)
        print(f"Invalid input. Please choose one of: {choices}")

# If more queries are added in the query wrapper, add them here to work in main automatically.
possibleQueries = [ReadQueryWrapper.GetStudentPerRoom,
                    ReadQueryWrapper.GetFiveLowestAvgAgeRooms,
                    ReadQueryWrapper.GetFiveLargestAgeDifferenceRooms,
                    ReadQueryWrapper.GetRoomsWithDifferentSexes]


# If more data converters are added, add them here to work in main automatically.
possibleConverters = [JsonConverter, XMLConverter]


def main(database: DatabaseConnection = None) -> None: # type: ignore
    



    database = database or PostgresConnection()
    try:
        connection = database.Connect()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    print("✅ Database connection successful")

    TableCreator(connection).CreateAll()
    
    while(True):
        print("\n1. Load the data")
        print("2. Query the database")
        print("3. Quit")
        
        var = PromptChoice("Enter appropriate number to choose your path: ", [1, 2, 3])
        if var == 1:
            # TBI: inputs for rooms and students
            try:
                LoadData(connection)
            except Exception as e:
                print(f"❌ Loading failed: {e}")
                return
        
        elif var == 2:
            print("\nPossible queries: ")
            for i, q in enumerate(possibleQueries, start=1):
                print(f"{i}. {(q.__doc__ or '').strip()}")

            var = PromptChoice("\nEnter the number to select a query: ", list(range(1, len(possibleQueries) + 1)))
            query = None
            qWrapper = ReadQueryWrapper(connection=connection)
            try:
                query = possibleQueries[var - 1](qWrapper)
            except Exception as e:
                print(f"Could not query the database: {e}")
                database.Close()
                return
            
            print("\nChoose the data output format:")
            for i, c in enumerate(possibleConverters, start=1):
                print(f"{i}. {(c.__doc__ or '').strip()}")

            var = PromptChoice("\nEnter the number to select the output format: ", list(range(1, len(possibleConverters) + 1)))
            result = possibleConverters[var - 1]().ConvertRecords(query) # type: ignore

            print(result)
            

        elif var == 3:
            database.Close()
            return

        
          



if __name__ == "__main__":
    main()
