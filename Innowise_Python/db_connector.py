import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
import psycopg2 as pg
from dotenv import load_dotenv

# Load variables from a .env file (if present) into the environment.
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection parameters for a database (holds configuration only)."""

    name: str = "innowise_python_postgres"
    user: str = "user"
    password: str = "password"
    host: str = "localhost"
    port: str = "5432"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Build configuration from environment variables (incl. a .env file)."""
        return cls(
            name=os.getenv("POSTGRES_DB", cls.name),
            user=os.getenv("POSTGRES_USER", cls.user),
            password=os.getenv("POSTGRES_PASSWORD", cls.password),
            host=os.getenv("POSTGRES_HOST", cls.host),
            port=os.getenv("POSTGRES_PORT", cls.port),
        )


class DatabaseConnection(ABC):
    """Abstraction for a database connection that callers depend on."""

    @abstractmethod
    def Connect(self):
        """Open (if needed) and return the underlying DB-API connection."""

    @abstractmethod
    def Close(self) -> None:
        """Close the connection if it is open. Safe to call repeatedly."""

    def __enter__(self):
        return self.Connect()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.Close()


class PostgresConnection(DatabaseConnection):
    """PostgreSQL implementation of DatabaseConnection."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self._config = config or DatabaseConfig.from_env()
        self._connection = None

    def Connect(self):
        if self._connection is None:
            self._connection = pg.connect(
                dbname=self._config.name,
                user=self._config.user,
                password=self._config.password,
                host=self._config.host,
                port=self._config.port,
            )
        return self._connection

    def Close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
