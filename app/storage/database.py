"""Persistência SQLite do log de operações e das tarefas de um nó."""
import hashlib
import json
import sqlite3
from pathlib import Path


class Database:
    def __init__(self, path: str):
        # Nos experimentos os contêineres não têm volume, então o diretório pode não existir.
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        # Cada fase da escrita só é confirmada depois de gravada em disco.
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT, status TEXT, version INTEGER);
            CREATE TABLE IF NOT EXISTS operations (
                request_id TEXT PRIMARY KEY, epoch INTEGER, sequence_number INTEGER, task TEXT, status TEXT
            );
        """)

    def list_tasks(self) -> list[dict]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM tasks ORDER BY id")]

    def get_task(self, task_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

    def list_operations(self) -> list[dict]:
        rows = self.connection.execute("SELECT * FROM operations ORDER BY epoch, sequence_number")
        return [self._operation(row) for row in rows]

    def get_operation(self, request_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM operations WHERE request_id = ?", (request_id,)).fetchone()
        return self._operation(row) if row else None

    def has_prepared(self) -> bool:
        return self.connection.execute("SELECT 1 FROM operations WHERE status = 'PREPARED'").fetchone() is not None

    def max_epoch(self) -> int:
        return self.connection.execute("SELECT COALESCE(MAX(epoch), 0) FROM operations").fetchone()[0]

    def next_sequence(self, epoch: int) -> int:
        query = "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM operations WHERE epoch = ?"
        return self.connection.execute(query, (epoch,)).fetchone()[0]

    def prepare(self, operation: dict) -> None:
        with self.connection:
            self._insert(operation, "PREPARED")

    def commit(self, request_id: str) -> None:
        task = self.get_operation(request_id)["task"]
        # Mesma transação: uma falha não deixa a tarefa alterada com a operação ainda PREPARED.
        with self.connection:
            self._save_task(task)
            self.connection.execute("UPDATE operations SET status = 'COMMITTED' WHERE request_id = ?", (request_id,))

    def install(self, operations: list[dict]) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM tasks")
            self.connection.execute("DELETE FROM operations")
            for operation in operations:
                self._insert(operation, "COMMITTED")
                self._save_task(operation["task"])

    def digests(self) -> tuple[str, str]:
        return digest(self.list_operations()), digest(self.list_tasks())

    def _operation(self, row: sqlite3.Row) -> dict:
        operation = dict(row)
        operation["task"] = json.loads(operation["task"])
        return operation

    def _insert(self, operation: dict, status: str) -> None:
        # OR IGNORE: um PREPARE repetido com a mesma Idempotency-Key não duplica a operação.
        self.connection.execute(
            "INSERT OR IGNORE INTO operations VALUES (?, ?, ?, ?, ?)",
            (operation["request_id"], operation["epoch"], operation["sequence_number"],
             json.dumps(operation["task"]), status),
        )

    def _save_task(self, task: dict) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO tasks VALUES (?, ?, ?, ?)",
            (task["id"], task["title"], task["status"], task["version"]),
        )


def digest(values: list[dict]) -> str:
    # sort_keys deixa o JSON, e portanto o hash, independente da ordem das chaves.
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
