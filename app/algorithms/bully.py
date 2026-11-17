"""Eleição de líder pelo algoritmo Bully."""
import asyncio
import logging

from app.communication.client import NodeClient
from app.config import Settings
from app.state import NodeState

logger = logging.getLogger(__name__)


class Bully:
    def __init__(self, settings: Settings, state: NodeState, client: NodeClient):
        self.settings = settings
        self.state = state
        self.client = client
        self.higher = [peer for peer in settings.peers if peer > settings.node_id]
        self.others = [peer for peer in settings.peers if peer != settings.node_id]
        self.task = None
        # Acorda a espera pelo COORDINATOR quando o líder ou o epoch mudam.
        self.changed = asyncio.Event()

    def start_election(self) -> None:
        # Pedidos simultâneos (várias suspeitas, ELECTIONs) compartilham a mesma rodada.
        if self._running() and self.state.leader_id is None:
            return
        self._begin(self.state.epoch + 1)

    def receive_election(self, message: dict) -> dict:
        # Se já disputa uma eleição tão recente quanto a do remetente, basta responder OK.
        if message["epoch"] > self.state.epoch or not self._running():
            self._begin(max(self.state.epoch, message["epoch"]))
        else:
            self._schedule()
        return {"type": "OK", "node_id": self.state.node_id, "epoch": self.state.epoch}

    def receive_coordinator(self, message: dict) -> bool:
        leader, epoch = message["leader_id"], message["epoch"]
        if epoch < self.state.epoch:
            return False
        if leader < self.state.node_id:
            # Um nó ativo com ID maior contesta o anúncio e disputa a liderança.
            if epoch > self.state.epoch:
                self._begin(epoch)
            else:
                self.start_election()
            return False
        # Em eleições simultâneas, mantém o maior líder já anunciado neste epoch.
        if epoch == self.state.epoch and self.state.leader_id is not None and leader < self.state.leader_id:
            return False
        self.state.recognize_leader(leader, epoch)
        self.changed.set()
        self.client.metrics.event("LEADER_RECOGNIZED", leader=leader, epoch=epoch)
        logger.info("COORDINATOR node=%s leader=%s epoch=%s", self.state.node_id, leader, epoch)
        return True

    def _running(self) -> bool:
        return self.task is not None and not self.task.done()

    def _schedule(self) -> None:
        if not self._running():
            self.task = asyncio.create_task(self._run())

    def _begin(self, epoch: int) -> None:
        self.state.begin_election(epoch)
        self.changed.set()
        self._schedule()

    async def _run(self) -> None:
        while True:
            epoch = self.state.epoch
            self.changed.clear()
            answers = await self._send_elections(epoch)
            # Um COORDINATOR pode chegar antes das respostas OK.
            if self.state.leader_id is not None:
                return
            if self.state.epoch != epoch:
                continue
            if answers:
                await self._wait_coordinator()
            else:
                await self._declare_victory(epoch)
            if self.state.leader_id is not None:
                return

    async def _send_elections(self, epoch: int) -> list[dict]:
        logger.info("ELECTION node=%s epoch=%s", self.state.node_id, epoch)
        self.client.metrics.event("ELECTION_ROUND")
        answers = await asyncio.gather(*(self.client.send_election(peer, epoch) for peer in self.higher))
        answers = [answer for answer in answers if answer is not None]
        # Um nó maior pode estar num epoch mais novo; a próxima rodada passa a usá-lo.
        newest = max([epoch] + [answer["epoch"] for answer in answers])
        if newest > self.state.epoch:
            self.state.begin_election(newest)
        return answers

    async def _wait_coordinator(self) -> None:
        try:
            await asyncio.wait_for(self.changed.wait(), self.settings.coordinator_timeout)
        except TimeoutError:
            # Quem respondeu OK falhou antes de anunciar a vitória; recomeça em um epoch novo.
            logger.info("Timeout aguardando COORDINATOR node=%s epoch=%s", self.state.node_id, self.state.epoch)
            self.state.begin_election(self.state.epoch + 1)

    async def _declare_victory(self, epoch: int) -> None:
        self.state.recognize_leader(self.state.node_id, epoch)
        self.client.metrics.event("LEADER_RECOGNIZED", leader=self.state.node_id, epoch=epoch)
        logger.info("LEADER_ELECTED node=%s epoch=%s", self.state.node_id, epoch)
        await asyncio.gather(*(self.client.send_coordinator(peer, epoch) for peer in self.others))
