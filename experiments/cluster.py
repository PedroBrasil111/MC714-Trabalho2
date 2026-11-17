"""Cluster Docker Compose com N nós: sobe os contêineres, injeta a falha, consulta os nós e derruba."""
import json
import subprocess
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "mc714-task-node:experiments"
PROJECT = "mc714-exp"
BASE_PORT = 18000
COMPOSE_FILE = Path(__file__).parent / "results" / "compose.json"
CONVERGENCE_TIMEOUT = 90.0
HEALTHCHECK = {
    "test": ["CMD", "python", "-c",
             "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=1)"],
    "interval": "1s", "timeout": "2s", "start_period": "5s", "retries": 30,
}


def run(*command: str) -> None:
    subprocess.run(command, check=True, capture_output=True)


def build_image() -> None:
    run("docker", "build", "-q", "-t", IMAGE, str(ROOT))


class Cluster:
    def __init__(self, n: int, failure_timeout: float):
        self.n = n
        self.failure_timeout = failure_timeout
        # a latência medida não inclui a abertura de uma conexão por escrita.
        self.http = httpx.Client(timeout=10.0, trust_env=False)

    def up(self) -> None:
        peers = ",".join(f"{i}=http://node{i}:8000" for i in range(1, self.n + 1))
        services = {}
        for i in range(1, self.n + 1):
            services[f"node{i}"] = {
                "image": IMAGE,
                "container_name": f"{PROJECT}-node{i}",
                "environment": {"NODE_ID": str(i), "PEERS": peers, "DB_PATH": "/data/tasks.db",
                                "FAILURE_TIMEOUT": str(self.failure_timeout)},
                "ports": [f"127.0.0.1:{BASE_PORT + i}:8000"],
                "healthcheck": HEALTHCHECK,
            }
        COMPOSE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Compose lê o arquivo diretamente
        COMPOSE_FILE.write_text(json.dumps({"name": PROJECT, "services": services}))
        # Remove containers que uma execução interrompida tenha deixado
        self._compose("down", "-v", "--remove-orphans", "-t", "1")
        self._compose("up", "-d", "--wait", "--wait-timeout", "120")

    def down(self) -> None:
        self.http.close()
        self._compose("down", "-v", "--remove-orphans", "-t", "1")

    def _compose(self, *args: str) -> None:
        run("docker", "compose", "-f", str(COMPOSE_FILE), *args)

    def kill(self, node: int) -> None:
        run("docker", "kill", "-s", "KILL", f"{PROJECT}-node{node}")

    def get(self, node: int, path: str):
        try:
            return self.http.get(f"http://127.0.0.1:{BASE_PORT + node}{path}", timeout=2.0).json()
        except (httpx.HTTPError, ValueError):
            return None

    def create_task(self, node: int, title: str, key: str) -> tuple[int, float]:
        started = time.perf_counter()
        try:
            response = self.http.post(f"http://127.0.0.1:{BASE_PORT + node}/tasks", json={"title": title},
                                      headers={"Idempotency-Key": key})
            status = response.status_code
        except httpx.HTTPError:
            status = 0
        return status, time.perf_counter() - started

    def metrics(self, nodes) -> dict[int, dict]:
        return {node: self.get(node, "/metrics") for node in nodes}

    def converged(self, alive: list[int], leader: int) -> bool:
        """Todos os vivos reconhecem o mesmo líder e epoch, sincronizados e com membros = vivos."""
        statuses = {node: self.get(node, "/status") for node in alive}
        if None in statuses.values():
            return False
        return len({status["epoch"] for status in statuses.values()}) == 1 and all(
            status["leader_id"] == leader and status["synchronized"] and status["replication_members"] == alive
            and status["role"] == ("PRIMARY" if node == leader else "BACKUP")
            for node, status in statuses.items())

    def wait_converged(self, alive: list[int], leader: int) -> float | None:
        """Instante (time.time) em que a convergência foi observada, ou None se o prazo acabar."""
        deadline = time.time() + CONVERGENCE_TIMEOUT
        while time.time() < deadline:
            if self.converged(alive, leader):
                return time.time()
            time.sleep(0.05)
        return None
