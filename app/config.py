"""Configuração do nó, lida das variáveis de ambiente definidas no Compose."""
import os


class Settings:
    def __init__(self):
        self.node_id = int(os.environ["NODE_ID"])
        self.peers = {}
        for entry in os.environ["PEERS"].split(","):
            peer_id, url = entry.split("=", 1)
            self.peers[int(peer_id)] = url
        self.db_path = os.environ["DB_PATH"]
        self.request_timeout = float(os.environ.get("REQUEST_TIMEOUT", "1.0"))
        self.coordinator_timeout = float(os.environ.get("COORDINATOR_TIMEOUT", "3.0"))
        self.heartbeat_interval = float(os.environ.get("HEARTBEAT_INTERVAL", "0.5"))
        self.failure_timeout = float(os.environ.get("FAILURE_TIMEOUT", "3.0"))
