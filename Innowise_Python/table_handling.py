from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence, Tuple, Dict

Record = Dict[str, Any]


@dataclass(frozen=True)
class ForeignKey:
    """Describes a foreign-key constraint on a single column."""

    column: str
    references_table: str
    references_column: str


@dataclass(frozen=True)
class TableMapping:
    """
    Describes how a record maps onto a database table.

    ``conflict_columns`` names the primary-key / unique columns used to detect
    a conflict. When set, the generated statement becomes an upsert
    (``ON CONFLICT ... DO UPDATE``). The same columns form the table's
    ``PRIMARY KEY`` when the table is created.

    ``column_types`` maps each column name to its SQL type definition and is
    only needed for table creation. ``foreign_keys`` lists referential
    constraints to emit in the ``CREATE TABLE`` statement.
    """

    table: str
    columns: Sequence[str]
    conflict_columns: Sequence[str] = ()
    column_types: Mapping[str, str] = field(default_factory=dict)
    foreign_keys: Sequence[ForeignKey] = ()

    
    def Upsert(self) -> str:
        placeholders = ", ".join(["%s"] * len(self.columns))
        column_list = ", ".join(self.columns)
        statement = (
            f"INSERT INTO {self.table} ({column_list}) VALUES ({placeholders})"
        )
        if not self.conflict_columns:
            return statement

        conflict = ", ".join(self.conflict_columns)
        updatable = [c for c in self.columns if c not in self.conflict_columns]
        if updatable:
            assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
            return f"{statement} ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"
        return f"{statement} ON CONFLICT ({conflict}) DO NOTHING"

    
    def CreateTable(self) -> str:
        """``CREATE TABLE IF NOT EXISTS`` statement derived from this mapping."""
        definitions = [
            f"{column} {self.column_types[column]}" for column in self.columns
        ]
        if self.conflict_columns:
            primary_key = ", ".join(self.conflict_columns)
            definitions.append(f"PRIMARY KEY ({primary_key})")
        for fk in self.foreign_keys:
            definitions.append(
                f"FOREIGN KEY ({fk.column}) "
                f"REFERENCES {fk.references_table} ({fk.references_column})"
            )
        body = ",\n    ".join(definitions)
        return f"CREATE TABLE IF NOT EXISTS {self.table} (\n    {body}\n)"

    def Row(self, record: Record) -> Tuple[Any, ...]:
        return tuple(record[column] for column in self.columns)


# Declarative mappings — add new entities here without touching DataLoader.
ROOMS = TableMapping(
    table="rooms",
    columns=("id", "name"),
    conflict_columns=("id",),
    column_types={
        "id": "INTEGER",
        "name": "VARCHAR(50)",
    },
)
STUDENTS = TableMapping(
    table="students",
    columns=("id", "name", "birthday", "room", "sex"),
    conflict_columns=("id",),
    column_types={
        "id": "INTEGER",
        "name": "VARCHAR(100)",
        "birthday": "TIMESTAMP",
        "room": "INT",
        "sex": "CHAR",
    },
    foreign_keys=(ForeignKey("room", "rooms", "id"),),
)



# When adding new entities, add them here to automatically handle creation and mapping in main.
# Order matters: tables are created in this order so referenced tables
# (e.g. rooms) exist before tables that reference them (e.g. students).
allMappings = [ROOMS, STUDENTS]


class TableCreator:
    """
    Creates appropriate tables in the database, based on available mappings, 
    if the tables do not exist.
    """

    def __init__(self, connection) -> None:
        self._connection = connection

    def CreateAll(self, mappings: Sequence[TableMapping] = allMappings) -> None:
        """
        Create every mapped table that does not already exist.

        Tables are created in the order given by ``mappings`` so that
        referenced tables precede the tables that reference them. Lets
        database errors propagate so the caller can decide how to react.
        """
        cursor = self._connection.cursor()
        for mapping in mappings:
            cursor.execute(mapping.CreateTable())
        self._connection.commit()
