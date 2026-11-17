"""Envio das mensagens entre nós por HTTP, com timeout e contagem para as métricas."""
import asyncio

import httpx

from app.config import Settings
from app.metrics import Metrics


class NodeClient:
    def __init__(self, settings: Settings, metrics: Metrics):
        self.settings = settings
        self.metrics = metrics
        # trust_env=False: as mensagens vão direto aos outros contêineres, sem proxies do ambiente.
        self.http = httpx.AsyncClient(timeout=settings.request_timeout, trust_env=False)

    async def send_heartbeat(self, peer_id: int) -> dict | None:
        return await self._request("HEARTBEAT", peer_id, "POST", "/internal/heartbeat",
                                   {"sender_id": self.settings.node_id})

    async def send_election(self, peer_id: int, epoch: int) -> dict | None:
        return await self._request("ELECTION", peer_id, "POST", "/internal/election",
                                   {"sender_id": self.settings.node_id, "epoch": epoch})

    async def send_coordinator(self, peer_id: int, epoch: int, kind: str = "COORDINATOR") -> dict | None:
        # kind separa o anúncio do Bully (COORDINATOR) do cercamento feito na recuperação (FENCE).
        return await self._request(kind, peer_id, "POST", "/internal/coordinator",
                                   {"leader_id": self.settings.node_id, "epoch": epoch})

    async def get_log(self, peer_id: int) -> list[dict] | None:
        return await self._request("RECOVERY_STATE", peer_id, "GET", "/internal/recovery/state")

    async def send_recovery(self, peer_id: int, phase: str, message: dict) -> dict | None:
        return await self._request(phase.upper(), peer_id, "POST", f"/internal/recovery/{phase}", message)

    async def send_replication(self, peer_id: int, phase: str, message: dict) -> dict | None:
        return await self._request(phase.upper(), peer_id, "POST", f"/internal/replication/{phase}", message)

    async def is_alive(self, peer_id: int) -> bool:
        try:
            response = await self.http.get(f"{self.settings.peers[peer_id]}/health")
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    async def _request(self, kind: str, peer_id: int, method: str, path: str, body: dict | None = None):
        """Retorna a resposta em JSON, ou None se o nó não respondeu com sucesso dentro do prazo."""
        self.metrics.sent[kind] += 1
        try:
            # O timeout do httpx vale para cada fase (conexão, leitura...), este limita a duração total
            async with asyncio.timeout(self.settings.request_timeout):
                response = await self.http.request(method, f"{self.settings.peers[peer_id]}{path}", json=body)
            response.raise_for_status()
        except (httpx.HTTPError, TimeoutError):
            return None
        self.metrics.acked[kind] += 1
        return response.json()
