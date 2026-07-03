import psycopg2
from json import JSONDecodeError
import data_loader
from db_connector import DatabaseConnection, PostgresConnection
from unloader import JsonConverter, XMLConverter, ReadQueryWrapper
from table_handling import TableCreator



def LoadData(connection, studentsPath: str, roomsPath: str) -> None:
    """Load the data from the given JSON files into the database."""
    loader = data_loader.DataLoader(connection)
  
    roomcount = loader.Load(data_loader.JsonFileDataSource(roomsPath), data_loader.ROOMS)
    print(f"Loaded {roomcount} rooms")
    studentcount = loader.Load(data_loader.JsonFileDataSource(studentsPath), data_loader.STUDENTS)
    print(f"Loaded {studentcount} students")


def PromptChoice(prompt: str, choices: list[int]) -> int:
    """Prompt until the user enters one of the allowed integer choices."""
    while True:
        var = input(prompt).strip()
        if var.isdigit() and int(var) in choices:
            return int(var)
        print(f"Invalid input. Please choose one of: {choices}")


def PromptPath(prompt: str, default: str) -> str:
    """Prompt for a file path, falling back to ``default`` on empty input."""
    var = input(f"{prompt} [{default}]: ").strip()
    return var or default

# If more queries are added in the query wrapper, add them here to work in main automatically.
possibleQueries = [ReadQueryWrapper.GetStudentPerRoom,
                    ReadQueryWrapper.GetFiveLowestAvgAgeRooms,
                    ReadQueryWrapper.GetFiveLargestAgeDifferenceRooms,
                    ReadQueryWrapper.GetRoomsWithDifferentSexes]


# If more data converters are added, add them here to work in main automatically.
possibleConverters = [JsonConverter, XMLConverter]


def main(database: DatabaseConnection | None = None) -> None: 
    



    database = database or PostgresConnection()
    try:
        with database as connection:
   

            print("✅ Database connection successful")

            try:
                TableCreator(connection).CreateAll()
            except (OSError, psycopg2.Error) as e:
                print(f"❌ Could not create tables: {e}")
                return

            while(True):
                print("\n1. Load the data \n2. Query the database \n3. Quit")
            
                var = PromptChoice("Enter appropriate number to make your choice: ", [1, 2, 3])

                # Load the data
                if var == 1:
                    roomsPath = PromptPath("Path to rooms file", "rooms.json")
                    studentsPath = PromptPath("Path to students file", "students.json")
                    try:
                        LoadData(connection, studentsPath, roomsPath)
                    except FileNotFoundError as e:
                        print(f"❌ File not found: {e}")
                    except JSONDecodeError as e:
                        print(f"❌ Invalid JSON: {e}")
                    except (KeyError, psycopg2.Error) as e:
                        print(f"❌ Loading failed: {e}")
                        return
                    except Exception as e:
                        print(f"An unexpected error occured: {e}")
                        return
                
                # Query the database
                elif var == 2:
                    print("\nPossible queries: ")
                    for i, q in enumerate(possibleQueries, start=1):
                        print(f"{i}. {(q.__doc__ or '').strip()}")

                    var = PromptChoice("\nEnter appropriate number to select a query: ", list(range(1, len(possibleQueries) + 1)))
                    query = None
                    qWrapper = ReadQueryWrapper(connection=connection)
                    try:
                        query = possibleQueries[var - 1](qWrapper)
                    except psycopg2.Error as e:
                        print(f"❌ Could not query the database: {e}")
                        return
                    except Exception as e:
                        print(f"An unexpected error occured: {e}")
                        return
                    
                    print("\nChoose the data output format:")
                    for i, c in enumerate(possibleConverters, start=1):
                        print(f"{i}. {(c.__doc__ or '').strip()}")

                    var = PromptChoice("\nEnter appropriate number to select the output format: ", list(range(1, len(possibleConverters) + 1)))
                    result = possibleConverters[var - 1]().ConvertRecords(query)

                    var = PromptChoice("\nChoose the output method:\n1. Print to console\n2. Save to file\nEnter the number to select the output method: ", [1, 2])
                    if var == 1:
                        print(result)
                    elif var == 2:
                        path = PromptPath("Enter the output file path (or leave empty to create in current directory)", "output.txt")
                        try:
                            with open(path, "w", encoding="utf-8") as f:
                                f.write(result)
                            print(f"✅ Output saved to {path}")
                        except OSError as e:
                            print(f"❌ Could not save to file: {e}")
                            return
                        except Exception as e:
                            print(f"An unexpected error occured: {e}")
                        return
                    
                # Exit the program
                elif var == 3:
                    return
    except psycopg2.OperationalError as ce:
        print(f"❌ Database connection failed: {ce}")
        return
    except Exception as e:
        print(f"An unexpected error occured: {e}")
        return
        
        
          



if __name__ == "__main__":
    main()
