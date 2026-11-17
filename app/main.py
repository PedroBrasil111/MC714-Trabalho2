"""API de tarefas e endpoints internos usados na comunicação entre os nós."""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header
from pydantic import BaseModel

from app.algorithms.bully import Bully
from app.algorithms.heartbeat import Heartbeat
from app.algorithms.recovery import Recovery
from app.algorithms.replication import PrimaryBackup
from app.communication.client import NodeClient
from app.config import Settings
from app.metrics import Metrics
from app.state import NodeState
from app.storage.database import Database

# O uvicorn só configura os próprios loggers; isto leva os logs da aplicação ao stdout do contêiner.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

settings = Settings()
database = Database(settings.db_path)
# Se o nó reiniciar com o banco anterior, o epoch continua do maior já registrado no log.
state = NodeState(settings.node_id, settings.peers, database.max_epoch())
client = NodeClient(settings, Metrics())
bully = Bully(settings, state, client)
heartbeat = Heartbeat(settings, state, client, bully)
# Escritas e recuperação usam a mesma trava, para que uma troca de configuração não interrompa uma escrita.
lock = asyncio.Lock()
replication = PrimaryBackup(settings, state, client, database, lock)
recovery = Recovery(settings, state, client, database, heartbeat, lock)


@asynccontextmanager
async def lifespan(app: FastAPI):
    bully.start_election()
    heartbeat.start()
    recovery.start()
    yield


app = FastAPI(title="Distributed Task Manager", lifespan=lifespan)


class TaskCreate(BaseModel):
    title: str
    status: str = "pending"


class TaskUpdate(BaseModel):
    title: str | None = None
    status: str | None = None


@app.post("/tasks", status_code=201)
async def create_task(command: TaskCreate, request_id: str | None = Header(None, alias="Idempotency-Key")):
    return await replication.write(command.model_dump(), None, request_id)


@app.get("/tasks")
async def list_tasks():
    return replication.list_tasks()


@app.put("/tasks/{task_id}")
async def update_task(task_id: str, command: TaskUpdate,
                      request_id: str | None = Header(None, alias="Idempotency-Key")):
    return await replication.write(command.model_dump(exclude_none=True), task_id, request_id)


@app.get("/health")
async def health():
    return {"status": "ok", "node_id": settings.node_id}


@app.get("/status")
async def status():
    others = [peer for peer in settings.peers if peer != settings.node_id]
    alive = await asyncio.gather(*(client.is_alive(peer) for peer in others))
    return {
        **state.snapshot(),
        "active_nodes": sorted([settings.node_id] + [peer for peer, ok in zip(others, alive) if ok]),
        "suspected_nodes": sorted(heartbeat.suspected),
        "replication_members": state.members,
        "synchronized": state.synchronized,
    }


@app.get("/metrics")
async def metrics():
    return client.metrics.snapshot()


@app.post("/internal/heartbeat")
async def receive_heartbeat(message: dict):
    heartbeat.receive(message["sender_id"])
    return {"status": "received"}


@app.post("/internal/election")
async def receive_election(message: dict):
    return bully.receive_election(message)


@app.post("/internal/coordinator")
async def receive_coordinator(message: dict):
    return {"accepted": bully.receive_coordinator(message)}


@app.get("/internal/replication/state")
async def replication_state():
    log_digest, tasks_digest = database.digests()
    return {**state.snapshot(), "log_digest": log_digest, "tasks_digest": tasks_digest,
            "has_prepared": database.has_prepared(), "synchronized": state.synchronized}


@app.post("/internal/replication/prepare")
async def receive_prepare(message: dict):
    return replication.receive_prepare(message)


@app.post("/internal/replication/commit")
async def receive_commit(message: dict):
    return replication.receive_commit(message)


@app.get("/internal/recovery/state")
async def recovery_state():
    return database.list_operations()


@app.post("/internal/recovery/synchronize")
async def receive_synchronize(message: dict):
    return recovery.receive_synchronize(message)


@app.post("/internal/recovery/configure")
async def receive_configure(message: dict):
    return recovery.receive_configure(message)
