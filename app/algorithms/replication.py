"""Replicação Primary-Backup síncrona, em duas fases (PREPARE e COMMIT)."""
import asyncio
import logging
from uuid import uuid4

from fastapi import HTTPException

from app.communication.client import NodeClient
from app.config import Settings
from app.state import NodeState
from app.storage.database import Database

logger = logging.getLogger(__name__)


class PrimaryBackup:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient, database: Database,
                 lock: asyncio.Lock):
        self.settings = settings
        self.state = state
        self.client = client
        self.database = database
        self.lock = lock

    def list_tasks(self) -> list[dict]:
        self._check_primary()
        return self.database.list_tasks()

    async def write(self, changes: dict, task_id: str | None, request_id: str | None) -> dict:
        request_id = request_id or str(uuid4())
        # A trava serializa as escritas, que recebem números de sequência consecutivos.
        async with self.lock:
            self._check_primary()
            operation = self.database.get_operation(request_id)
            if operation is None:
                operation = self._prepare_local(changes, task_id, request_id)
            elif operation["status"] == "COMMITTED":
                return operation["task"]
            logger.info("PREPARE node=%s epoch=%s sequence=%s request_id=%s", self.state.node_id,
                        operation["epoch"], operation["sequence_number"], request_id)
            await self._replicate("prepare", {"sender_id": self.state.node_id, "epoch": operation["epoch"],
                                              "operation": operation})
            await self._replicate("commit", {"sender_id": self.state.node_id, "epoch": operation["epoch"],
                                             "request_id": request_id})
            # O primário confirma por último: se ele tem COMMITTED, todos os backups também têm.
            self.database.commit(request_id)
            logger.info("COMMIT node=%s epoch=%s sequence=%s request_id=%s", self.state.node_id,
                        operation["epoch"], operation["sequence_number"], request_id)
            return operation["task"]

    def receive_prepare(self, message: dict) -> dict:
        self._check_leader(message)
        self.database.prepare(message["operation"])
        return {"node_id": self.state.node_id, "status": "PREPARED"}

    def receive_commit(self, message: dict) -> dict:
        self._check_leader(message)
        self.database.commit(message["request_id"])
        return {"node_id": self.state.node_id, "status": "COMMITTED"}

    def _prepare_local(self, changes: dict, task_id: str | None, request_id: str) -> dict:
        if self.database.has_prepared():
            raise HTTPException(503, "Há uma escrita com resultado desconhecido; repita-a com a mesma Idempotency-Key")
        if task_id is None:
            task = {"id": f"task-{uuid4().hex}", **changes, "version": 1}
        else:
            current = self.database.get_task(task_id)
            if current is None:
                raise HTTPException(404, "Tarefa não encontrada")
            task = {**current, **changes, "version": current["version"] + 1}
        operation = {"request_id": request_id, "epoch": self.state.epoch,
                     "sequence_number": self.database.next_sequence(self.state.epoch), "task": task}
        self.database.prepare(operation)
        return operation

    async def _replicate(self, phase: str, message: dict) -> None:
        backups = [peer for peer in self.state.members if peer != self.state.node_id]
        answers = await asyncio.gather(*(self.client.send_replication(peer, phase, message) for peer in backups))
        if None in answers:
            # Alguns backups podem ter aplicado a fase; a operação fica pendente até o cliente repeti-la.
            raise HTTPException(503, f"{phase.upper()} sem confirmação de todos os backups; resultado desconhecido")

    def _check_primary(self) -> None:
        leader = self.state.leader_id
        if leader is not None and leader != self.state.node_id:
            raise HTTPException(409, {"error": "Este nó é backup; envie a requisição ao líder",
                                      "leader_id": leader, "leader_url": self.settings.peers[leader]})
        if self.state.role != "PRIMARY":
            raise HTTPException(503, "Primário indisponível durante a eleição ou a recuperação")

    def _check_leader(self, message: dict) -> None:
        # synchronized só é verdadeiro depois que a configuração do epoch atual foi instalada.
        if not (self.state.synchronized and self.state.leader_id == message["sender_id"]
                and self.state.epoch == message["epoch"]):
            raise HTTPException(409, "Replicação rejeitada: líder ou epoch diferente do reconhecido")
