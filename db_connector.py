import psycopg2 as pg

class DbConnector:
    def __init__(self):
        pass
    
    def CreateConnection(db_name = "innowise_python_postgres", db_user = "user", db_password = "password", db_host="localhost", db_port="5432"):
        """
        Create a connection to the PostgreSQL database. (NEEDS TO ACCEPT OTHER DATABASE TYPES IN THE FUTURE)
        """
        try:
            conn = pg.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            print("✅ Connection successful")
            return conn
        except pg.OperationalError as e:
            print(f"Error: {e}")
            return None
        
    def CloseConnection(conn):
        """
        Close the connection to the database.
        """
        if conn:
            conn.close()
            print("✅ Connection closed")

