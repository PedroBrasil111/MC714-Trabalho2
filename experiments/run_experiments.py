"""Experimentos do Bully, do Primary-Backup e do Heartbeat.

Cada execução sobe um cluster Docker novo (bancos vazios), aplica a carga e a falha do cenário,
coleta as métricas dos nós e derruba o cluster. Cada experimento gera uma linha por execução
em <out>/<experimento>.csv; execuções já registradas são puladas.

Uso: python experiments/run_experiments.py [--exp bully|pb|hb] [--reps 10]
"""
import csv
import random
import statistics
import time
from argparse import ArgumentParser
from operator import itemgetter
from pathlib import Path

from cluster import Cluster, build_image

LEVELS = {"bully": (3, 5, 7), "pb": (3, 5, 7), "hb": (1.5, 2.5, 4.0)}
SEEDS = [1000 * i for i in range(1, 11)]
COLUMNS = {
    "bully": ["level", "seed", "ok", "startup_s", "duration_s", "kill_s", "messages", "election_time_s"],
    "pb": ["level", "seed", "ok", "startup_s", "duration_s", "messages_per_op", "latency_mean_ms",
           "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "throughput_ops_s", "heartbeats_per_s"],
    "hb": ["level", "seed", "ok", "startup_s", "duration_s", "kill_s", "detection_s", "silence_min_s",
           "silence_max_s", "false_suspicions", "heartbeats_per_node_s"],
}
HEARTBEAT_INTERVAL = 0.5  # padrão do app
FAILURE_TIMEOUT = 3.0     # bully e pb, no heartbeat é variado
PRE_TASKS = 20
WARMUP_OPS = 50
OPS = 1000
HB_NODES = 3
WINDOW = 60.0
LOAD_RATE = 10.0


def write_tasks(cluster: Cluster, node: int, count: int, prefix: str, rng: random.Random) -> list[tuple[int, float]]:
    return [cluster.create_task(node, f"{prefix}-{i}-" + "x" * rng.randint(1, 64), f"{prefix}-{i}")
            for i in range(count)]


def write_at_rate(cluster: Cluster, node: int, prefix: str, rng: random.Random) -> None:
    end = time.time() + WINDOW
    next_at = time.time()
    i = 0
    while time.time() < end:
        write_tasks(cluster, node, 1, f"{prefix}-{i}", rng)
        i += 1
        next_at += 1 / LOAD_RATE
        time.sleep(max(0.0, next_at - time.time()))


def kill_leader(cluster: Cluster, rng: random.Random) -> tuple[float, float, bool]:
    """Encerra o líder (nó N) e espera os sobreviventes convergirem para o nó N-1."""
    # Atraso aleatório para que a falha ocorra em qualquer ponto do ciclo de heartbeat
    time.sleep(rng.uniform(0, HEARTBEAT_INTERVAL))
    killed_at = time.time()
    cluster.kill(cluster.n)
    kill_s = time.time() - killed_at
    converged = cluster.wait_converged(list(range(1, cluster.n)), cluster.n - 1) is not None
    return killed_at, kill_s, converged


def events(snapshots: dict, name: str, since: float, until: float = float("inf")) -> list[dict]:
    found = [{**event, "node": node} for node, snapshot in snapshots.items() for event in snapshot["events"]
             if event["event"] == name and since <= event["t"] <= until]
    return sorted(found, key=itemgetter("t"))


def count(before: dict, after: dict, field: str, kinds: list[str]) -> int:
    return sum(after[node][field].get(kind, 0) - before[node][field].get(kind, 0) for node in after for kind in kinds)


def consistent(cluster: Cluster, nodes: list[int]) -> bool:
    """Mesmos hashes de log e tarefas em todos os nós e nenhuma operação pendente."""
    states = [cluster.get(node, "/internal/replication/state") for node in nodes]
    return len({(state["log_digest"], state["tasks_digest"]) for state in states}) == 1 and not any(
        state["has_prepared"] for state in states)


def election_time(after: dict, killed_at: float, leader: int, epoch: int) -> float:
    """Do início da primeira rodada até o último sobrevivente reconhecer o novo líder."""
    start = events(after, "ELECTION_ROUND", killed_at)[0]["t"]
    recognized = {}
    for event in events(after, "LEADER_RECOGNIZED", killed_at):
        if event["leader"] == leader and event["epoch"] == epoch:
            recognized.setdefault(event["node"], event["t"])
    return max(recognized.values()) - start


def scenario_bully(cluster: Cluster, seed: int) -> dict:
    n = cluster.n
    rng, kill_rng = random.Random(seed), random.Random(seed + 1)
    writes = write_tasks(cluster, n, PRE_TASKS, f"bully-{n}-{seed}-pre", rng)
    survivors = list(range(1, n))
    before = cluster.metrics(survivors)
    killed_at, kill_s, converged = kill_leader(cluster, kill_rng)
    if not converged:
        return {"ok": False, "kill_s": kill_s}
    after = cluster.metrics(survivors)
    epoch = cluster.get(n - 1, "/status")["epoch"]
    # novo primário mantém tarefas confirmadas e volta aceitar escritas
    writes += write_tasks(cluster, n - 1, 1, f"bully-{n}-{seed}-post", rng)
    tasks = cluster.get(n - 1, "/tasks")
    ok = all(status == 201 for status, _ in writes) and len(tasks) == PRE_TASKS + 1 and consistent(cluster, survivors)
    return {
        "ok": ok,
        "kill_s": kill_s,
        "messages": count(before, after, "sent", ["ELECTION", "COORDINATOR"]) + count(before, after, "acked", ["ELECTION"]),
        "election_time_s": election_time(after, killed_at, n - 1, epoch),
    }


def scenario_pb(cluster: Cluster, seed: int) -> dict:
    n = cluster.n
    nodes = list(range(1, n + 1))
    rng = random.Random(seed)
    write_tasks(cluster, n, WARMUP_OPS, f"pb-{n}-{seed}-warmup", rng)
    before = cluster.metrics(nodes)
    started = time.time()
    writes = write_tasks(cluster, n, OPS, f"pb-{n}-{seed}", rng)
    duration = time.time() - started
    after = cluster.metrics(nodes)
    latencies = [latency * 1000 for status, latency in writes if status == 201]
    percentiles = statistics.quantiles(latencies, n=100, method="inclusive")
    replication = count(before, after, "sent", ["PREPARE", "COMMIT"]) + count(before, after, "acked", ["PREPARE", "COMMIT"])
    tasks = cluster.get(n, "/tasks")
    return {
        "ok": len(latencies) == OPS and len(tasks) == WARMUP_OPS + OPS and consistent(cluster, nodes),
        "messages_per_op": replication / len(latencies),
        "latency_mean_ms": statistics.mean(latencies),
        "latency_p50_ms": percentiles[49],
        "latency_p95_ms": percentiles[94],
        "latency_p99_ms": percentiles[98],
        "throughput_ops_s": len(latencies) / duration,
        "heartbeats_per_s": count(before, after, "sent", ["HEARTBEAT"]) / duration,
    }


def scenario_hb(cluster: Cluster, seed: int) -> dict:
    n = cluster.n
    nodes = list(range(1, n + 1))
    rng, kill_rng = random.Random(seed), random.Random(seed + 1)
    before = cluster.metrics(nodes)
    window_start = time.time()
    write_at_rate(cluster, n, f"hb-{seed}", rng)
    window_end = time.time()
    window_metrics = cluster.metrics(nodes)
    killed_at, kill_s, converged = kill_leader(cluster, kill_rng)
    after = cluster.metrics(nodes[:-1])
    suspicions = {}
    for event in events(after, "SUSPECTED", killed_at):
        if event["peer"] == n:
            suspicions.setdefault(event["node"], event)
    silences = [event["silence"] for event in suspicions.values()]
    return {
        # todos os sobreviventes suspeitam do lider só depois de T segundos sem heartbeats
        "ok": converged and len(suspicions) == n - 1 and min(silences) > cluster.failure_timeout,
        "kill_s": kill_s,
        "detection_s": min(event["t"] for event in suspicions.values()) - killed_at,
        "silence_min_s": min(silences),
        "silence_max_s": max(silences),
        "false_suspicions": len(events(window_metrics, "SUSPECTED", window_start, window_end)),
        "heartbeats_per_node_s": count(before, window_metrics, "sent", ["HEARTBEAT"]) / (window_end - window_start) / n,
    }


SCENARIOS = {"bully": scenario_bully, "pb": scenario_pb, "hb": scenario_hb}


def run_once(experiment: str, level: float, seed: int) -> dict:
    n = HB_NODES if experiment == "hb" else level
    cluster = Cluster(n, level if experiment == "hb" else FAILURE_TIMEOUT)
    started = time.time()
    try:
        cluster.up()
        # medição só começa depois da eleição inicial e ativação do primário
        ready_at = cluster.wait_converged(list(range(1, n + 1)), n)
        if ready_at is None:
            raise RuntimeError(f"{experiment} {level} seed={seed}: o cluster não convergiu na inicialização")
        row = SCENARIOS[experiment](cluster, seed)
    finally:
        cluster.down()
    return {"level": level, "seed": seed, "startup_s": ready_at - started, "duration_s": time.time() - started, **row}


def completed(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    with open(path, newline="") as file:
        return {(row["level"], row["seed"]) for row in csv.DictReader(file)}


def append_row(path: Path, columns: list[str], row: dict) -> None:
    new_file = not path.exists()
    with open(path, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns, restval="")
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = ArgumentParser(description="Experimentos do Bully, do Primary-Backup e do Heartbeat")
    parser.add_argument("--exp", choices=["all", *LEVELS], default="all")
    parser.add_argument("--reps", type=int, default=len(SEEDS), help="execuções por nível (máx. 10)")
    parser.add_argument("--out", default=str(Path(__file__).parent / "results" / "raw"))
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    build_image()
    for experiment in LEVELS if args.exp == "all" else [args.exp]:
        path = out / f"{experiment}.csv"
        done = completed(path)
        # intercalados em cada semente
        for seed in SEEDS[:args.reps]:
            for level in LEVELS[experiment]:
                if (str(level), str(seed)) in done:
                    continue
                row = run_once(experiment, level, seed)
                append_row(path, COLUMNS[experiment], row)
                print(f"{experiment} level={level} seed={seed}: {'ok' if row['ok'] else 'FALHOU'} "
                      f"({row['duration_s']:.1f} s)", flush=True)


if __name__ == "__main__":
    main()
