"""Contadores de mensagens e eventos usados pelos experimentos."""
import time
from collections import Counter


class Metrics:
    def __init__(self):
        self.sent = Counter()
        self.acked = Counter()
        self.events = []

    def event(self, name: str, **data) -> None:
        # time.time() em vez de monotonic(): os contêineres compartilham o relógio do host,
        # então instantes registrados em nós diferentes podem ser comparados.
        self.events.append({"t": time.time(), "event": name, **data})

    def snapshot(self) -> dict:
        return {"sent": dict(self.sent), "acked": dict(self.acked), "events": self.events}
