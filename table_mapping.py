from dataclasses import dataclass
from typing import Any, Sequence, Tuple, Dict

Record = Dict[str, Any]

@dataclass(frozen=True)
class TableMapping:
    """Describes how a record maps onto a database table.

    ``conflict_columns`` names the primary-key / unique columns used to detect
    a conflict. When set, the generated statement becomes an upsert
    (``ON CONFLICT ... DO UPDATE``); when empty it is a plain ``INSERT``.
    """

    table: str
    columns: Sequence[str]
    conflict_columns: Sequence[str] = ()

    @property
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
