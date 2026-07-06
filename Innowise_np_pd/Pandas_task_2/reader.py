# Used to load the data from the excel file to a Postgres database.
# Plain psycopg2 - no SQLAlchemy.

from pathlib import Path

import pandas as pd
import psycopg2 as pg
from psycopg2.extras import execute_values

path = Path(__file__).parent / "task_2_data_ex.xlsx"

data = pd.read_excel(path)

### load to Postgres database

# DB connection params 
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_DB = "innowise_pandas_postgres"
HOST = "localhost"
PORT = "5432"

TABLE_NAME = "bom_data"

# Schema derived from the Excel columns / dtypes.

COLUMNS = [
    ("year", "INTEGER"),
    ("month", "INTEGER"),
    ("produced_material", "BIGINT"),
    ("produced_material_production_type", "INTEGER"),
    ("produced_material_release_type", "TEXT"),
    ("produced_material_quantity", "NUMERIC"),
    ("component_material", "BIGINT"),
    ("component_material_production_type", "INTEGER"),
    ("component_material_release_type", "TEXT"),
    ("component_material_quantity", "NUMERIC"),
    ("plant_id", "TEXT"),
]

column_names = [name for name, _ in COLUMNS]
create_table_sql = "CREATE TABLE {table} (\n    {cols}\n);".format(
    table=TABLE_NAME,
    cols=",\n    ".join(f"{name} {sql_type}" for name, sql_type in COLUMNS),
)



# Align the DataFrame to the schema column order and turn NaN into None
# so psycopg2 writes proper SQL NULLs.
frame = data[column_names].astype(object).where(pd.notnull(data[column_names]), None)
rows = list(frame.itertuples(index=False, name=None))

insert_sql = f"INSERT INTO {TABLE_NAME} ({', '.join(column_names)}) VALUES %s"

connection = pg.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=HOST,
    port=PORT,
)

try:
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
            cursor.execute(create_table_sql)
            execute_values(cursor, insert_sql, rows)
    print(f"Loaded {len(rows)} rows into '{TABLE_NAME}'.")
finally:
    connection.close()
