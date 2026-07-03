import json
from abc import ABC, abstractmethod
from typing import List

from table_handling import TableMapping, Record, ROOMS, STUDENTS




class DataSource(ABC):
    """Abstraction over a source of records."""

    @abstractmethod
    def Read(self) -> List[Record]:
        """Return the records to be loaded."""


class JsonFileDataSource(DataSource):
    """Reads a list of records from a JSON file."""

    def __init__(self, path: str) -> None:
        self._path = path

    def Read(self) -> List[Record]:
        with open(self._path, "r", encoding="utf-8") as file:
            return json.load(file)




class DataLoader:
    """Loads records from a :class:`DataSource` into a mapped table."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def Load(self, source: DataSource, mapping: TableMapping) -> int:
        """Insert every record from ``source`` into ``mapping``'s table.

        Returns the number of rows inserted. Lets database/IO errors propagate
        so the caller can decide how to handle a failed load.
        """
        rows = [mapping.Row(record) for record in source.Read()]
        cursor = self._connection.cursor()
        cursor.executemany(mapping.Upsert(), rows)
        self._connection.commit()
        return cursor.rowcount


