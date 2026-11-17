"""Detector de falhas: suspeita de um nó que passa mais de FAILURE_TIMEOUT sem enviar heartbeats."""
import asyncio
import logging
import time

from app.algorithms.bully import Bully
from app.communication.client import NodeClient
from app.config import Settings
from app.state import NodeState

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 0.1


class Heartbeat:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient, bully: Bully):
        self.settings = settings
        self.state = state
        self.client = client
        self.bully = bully
        self.peers = [peer for peer in settings.peers if peer != settings.node_id]
        # O prazo começa a contar na inicialização, para que nenhum nó seja suspeito antes de enviar o primeiro heartbeat.
        now = time.monotonic()
        self.last_received = {peer: now for peer in self.peers}
        self.suspected = set()
        self.failed_leader = None
        self.tasks = []

    def start(self) -> None:
        # Um laço de envio por destino, para que um nó lento não atrase os heartbeats aos demais.
        self.tasks = [asyncio.create_task(self._send_loop(peer)) for peer in self.peers]
        self.tasks.append(asyncio.create_task(self._monitor_loop()))

    def receive(self, sender_id: int) -> None:
        self.last_received[sender_id] = time.monotonic()
        if sender_id in self.suspected:
            self.suspected.remove(sender_id)
            if self.failed_leader is not None and self.failed_leader[0] == sender_id:
                self.failed_leader = None
            logger.info("UNSUSPECTED node=%s peer=%s", self.state.node_id, sender_id)

    async def _send_loop(self, peer_id: int) -> None:
        while True:
            started = time.monotonic()
            await self.client.send_heartbeat(peer_id)
            # Desconta a duração do envio para manter a cadência de HEARTBEAT_INTERVAL.
            await asyncio.sleep(max(0, self.settings.heartbeat_interval - (time.monotonic() - started)))

    async def _monitor_loop(self) -> None:
        while True:
            self._check_peers()
            self._check_leader()
            await asyncio.sleep(CHECK_INTERVAL)

    def _check_peers(self) -> None:
        now = time.monotonic()
        for peer, last in self.last_received.items():
            if peer not in self.suspected and now - last > self.settings.failure_timeout:
                self.suspected.add(peer)
                self.client.metrics.event("SUSPECTED", peer=peer, silence=now - last)
                logger.info("SUSPECTED node=%s peer=%s", self.state.node_id, peer)

    def _check_leader(self) -> None:
        leadership = (self.state.leader_id, self.state.epoch)
        # A suspeita continua ativa nas verificações seguintes; guardar (líder, epoch) evita
        # iniciar uma eleição a cada verificação.
        if self.state.leader_id in self.suspected and leadership != self.failed_leader:
            self.failed_leader = leadership
            self.bully.start_election()
