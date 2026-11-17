"""Recuperação: sincroniza as réplicas e instala uma nova configuração antes de ativar o primário."""
import asyncio
import logging
from operator import itemgetter

from fastapi import HTTPException

from app.algorithms.heartbeat import Heartbeat
from app.communication.client import NodeClient
from app.config import Settings
from app.state import NodeState
from app.storage.database import Database

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 0.1


def merge_committed(logs: list[list[dict]]) -> list[dict]:
    """Mantém as operações COMMITTED em algum sobrevivente e descarta as que só estão PREPARED."""
    committed = {}
    for log in logs:
        for operation in log:
            if operation["status"] == "COMMITTED":
                committed[operation["request_id"]] = operation
    return sorted(committed.values(), key=itemgetter("epoch", "sequence_number"))


class Recovery:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient, database: Database,
                 heartbeat: Heartbeat, lock: asyncio.Lock):
        self.settings = settings
        self.state = state
        self.client = client
        self.database = database
        self.heartbeat = heartbeat
        self.lock = lock
        self.task = None

    def start(self) -> None:
        self.task = asyncio.create_task(self._loop())

    def receive_synchronize(self, message: dict) -> dict:
        self._check_leader(message)
        self.database.install(message["operations"])
        # Os hashes confirmam que o log e as tarefas ficaram iguais aos do líder.
        if self.database.digests() != (message["log_digest"], message["tasks_digest"]):
            raise HTTPException(409, "Hashes diferentes dos do líder após a sincronização")
        self.state.members = message["members"]
        return {"node_id": self.state.node_id, "status": "SYNCHRONIZED"}

    def receive_configure(self, message: dict) -> dict:
        self._check_leader(message)
        self.state.synchronized = True
        return {"node_id": self.state.node_id, "status": "CONFIRMED"}

    async def _loop(self) -> None:
        while True:
            if self._needed():
                # Espera a escrita em andamento terminar antes de trocar a configuração.
                async with self.lock:
                    await self._recover()
            await asyncio.sleep(CHECK_INTERVAL)

    def _needed(self) -> bool:
        if self.state.role == "LEADER_ELECTED":
            return True
        return self.state.role == "PRIMARY" and bool(set(self.state.members) & self.heartbeat.suspected)

    async def _recover(self) -> None:
        # O estado pode ter mudado enquanto a trava estava ocupada.
        if not self._needed():
            return
        if self.state.role == "PRIMARY":
            # Falha de backup: o primário continua líder, mas sobe o epoch para invalidar a configuração antiga.
            self.state.recognize_leader(self.state.node_id, self.state.epoch + 1)
        epoch = self.state.epoch
        members = sorted(set(self.state.members) - self.heartbeat.suspected)
        backups = [peer for peer in members if peer != self.state.node_id]

        operations = await self._collect_operations(epoch, backups)
        if operations is None:
            return
        self.database.install(operations)
        log_digest, tasks_digest = self.database.digests()
        synchronize = {"sender_id": self.state.node_id, "epoch": epoch, "members": members,
                       "operations": operations, "log_digest": log_digest, "tasks_digest": tasks_digest}
        if not await self._send_all(backups, "synchronize", synchronize, epoch):
            return
        if not await self._send_all(backups, "configure", {"sender_id": self.state.node_id, "epoch": epoch}, epoch):
            return
        self._activate(epoch, members, len(operations))

    async def _collect_operations(self, epoch: int, backups: list[int]) -> list[dict] | None:
        # FENCE: os backups passam a reconhecer este líder no epoch atual e recusam mensagens do anterior.
        await asyncio.gather(*(self.client.send_coordinator(peer, epoch, "FENCE") for peer in backups))
        logs = await asyncio.gather(*(self.client.get_log(peer) for peer in backups))
        if None in logs or not self._still_candidate(epoch):
            return None
        return merge_committed([self.database.list_operations(), *logs])

    async def _send_all(self, backups: list[int], phase: str, message: dict, epoch: int) -> bool:
        answers = await asyncio.gather(*(self.client.send_recovery(peer, phase, message) for peer in backups))
        # Uma nova eleição pode ter começado durante os envios.
        return None not in answers and self._still_candidate(epoch)

    def _still_candidate(self, epoch: int) -> bool:
        return self.state.role == "LEADER_ELECTED" and self.state.epoch == epoch

    def _activate(self, epoch: int, members: list[int], recovered: int) -> None:
        self.state.members = members
        self.state.synchronized = True
        self.state.role = "PRIMARY"
        logger.info("PRIMARY node=%s epoch=%s members=%s recovered=%s", self.state.node_id, epoch, members, recovered)

    def _check_leader(self, message: dict) -> None:
        if not (self.state.role == "BACKUP" and self.state.leader_id == message["sender_id"]
                and self.state.epoch == message["epoch"]):
            raise HTTPException(409, "Configuração de um líder ou epoch não reconhecido")
