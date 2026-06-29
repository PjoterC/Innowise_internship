import data_loader
import db_connector


def main():
    conn = db_connector.DbConnector.CreateConnection()
    if conn:
        data_loader.DataLoader(conn).LoadRooms("rooms.json")
        data_loader.DataLoader(conn).LoadStudents("students.json")
        db_connector.DbConnector.CloseConnection(conn)
    
main()