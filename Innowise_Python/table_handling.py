import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Sequence, Tuple, Dict

from psycopg2 import sql

Record = Dict[str, Any]


class SqlDialect(ABC):
    """Abstract base for database-specific SQL generation.

    A dialect builds the statements whose syntax differs between database
    modules/drivers. Each concrete dialect is expected to compose statements
    with its driver's own SQL-building facilities, so that identifiers and
    values are quoted/bound by the library and injection is impossible.
    """

    @abstractmethod
    def Upsert(
        self,
        table: str,
        columns: Sequence[str],
        conflict_columns: Sequence[str],
    ) -> Any:
        """Build an executable, injection-safe INSERT-or-update statement."""


class PostgresDialect(SqlDialect):
    """PostgreSQL dialect for the psycopg2 driver.

    Statements are composed with :mod:`psycopg2.sql`, so table and column
    names go in as :class:`~psycopg2.sql.Identifier` (quoted and escaped by
    the driver) and values as :class:`~psycopg2.sql.Placeholder`. The returned
    ``Composed`` object is accepted directly by ``cursor.execute`` /
    ``cursor.executemany``.
    """

    def Upsert(
        self,
        table: str,
        columns: Sequence[str],
        conflict_columns: Sequence[str],
    ) -> sql.Composed:
        statement = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({values})").format(
            table=sql.Identifier(table),
            columns=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            values=sql.SQL(", ").join([sql.Placeholder()] * len(columns)),
        )
        if not conflict_columns:
            return sql.Composed([statement])

        conflict = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns)
        updatable = [c for c in columns if c not in conflict_columns]
        if updatable:
            assignments = sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in updatable
            )
            return statement + sql.SQL(
                " ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"
            ).format(conflict=conflict, assignments=assignments)
        return statement + sql.SQL(" ON CONFLICT ({conflict}) DO NOTHING").format(
            conflict=conflict
        )


# Default dialect used by mappings that don't specify one.
DEFAULT_DIALECT: SqlDialect = PostgresDialect()


@dataclass(frozen=True)
class TableMapping:
    """Describes how a record maps onto a database table.

    ``conflict_columns`` names the primary-key / unique columns used to detect
    a conflict. When set, the generated statement becomes an upsert
    (``ON CONFLICT ... DO UPDATE``).

    ``dialect`` selects the database-specific SQL generation and defaults to
    PostgreSQL/psycopg2; pass another :class:`SqlDialect` to target a different
    backend.
    """

    table: str
    columns: Sequence[str]
    conflict_columns: Sequence[str] = ()
    dialect: SqlDialect = field(default=DEFAULT_DIALECT, compare=False, repr=False)

    def Upsert(self) -> Any:
        return self.dialect.Upsert(self.table, self.columns, self.conflict_columns)

    def Row(self, record: Record) -> Tuple[Any, ...]:
        return tuple(record[column] for column in self.columns)


# Declarative mappings — add new entities here without touching DataLoader.
ROOMS = TableMapping(
    table="rooms",
    columns=("id", "name"),
    conflict_columns=("id",),
)
STUDENTS = TableMapping(
    table="students",
    columns=("id", "name", "birthday", "room", "sex"),
    conflict_columns=("id",),
)


class TableCreator:
    """Ensures the required tables exist by running the schema SQL script.
    
    The script uses
    ``CREATE TABLE IF NOT EXISTS``, so running it on every startup is safe.
    """

    SCHEMA_PATH = os.path.join(
        os.path.dirname(__file__), "SQLqueries", "create_tables.sql"
    )

    def __init__(self, connection) -> None:
        self._connection = connection

    def CreateAll(self, path: str = SCHEMA_PATH) -> None:
        """Execute the schema script, creating any tables that don't exist."""
        with open(path, "r", encoding="utf-8") as file:
            script = file.read()
        cursor = self._connection.cursor()
        cursor.execute(script)
        self._connection.commit()
