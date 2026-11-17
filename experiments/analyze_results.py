"""Gera a figura do relatório e imprime as médias ± IC95 citadas no texto e nas tabelas.

Uso: python experiments/analyze_results.py
"""
import csv
import math
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

RAW = Path(__file__).parent / "results" / "raw"
FIGURE = Path(__file__).resolve().parent.parent / "relatorio" / "images" / "bully_mensagens"
# t de Student bicaudal de 95%, por graus de liberdade (execuções - 1)
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}
HEARTBEAT_INTERVAL = 0.5
CHECK_INTERVAL = 0.1


def load(experiment: str) -> tuple[int, list[dict]]:
    """Número de execuções e as execuções corretas, com os valores convertidos para float."""
    with open(RAW / f"{experiment}.csv", newline="") as file:
        rows = list(csv.DictReader(file))
    correct = [{key: float(value) for key, value in row.items() if key != "ok"} for row in rows if row["ok"] == "True"]
    return len(rows), correct


def by_level(rows: list[dict]) -> dict[float, list[dict]]:
    groups = {}
    for row in rows:
        groups.setdefault(row["level"], []).append(row)
    return dict(sorted(groups.items()))


def mean_ci(values: list[float]) -> tuple[float, float]:
    if len(values) < 2:
        return values[0], 0.0
    return statistics.mean(values), T95[len(values) - 1] * statistics.stdev(values) / math.sqrt(len(values))


def fmt(rows: list[dict], key: str, digits: int = 1, scale: float = 1.0) -> str:
    mean, ci = mean_ci([row[key] * scale for row in rows])
    return f"{key}={mean:.{digits}f} ± {ci:.{digits}f}"


def bully_figure(rows: list[dict]) -> None:
    fig, ax = plt.subplots(figsize=(6, 3.6))
    grid = np.linspace(2.7, 7.3, 200)
    ax.plot(grid, grid * (grid - 1), color="black", linewidth=1.8, label=r"Pior caso: $N(N-1)$")
    ax.plot(grid, grid, color="black", linewidth=1.8, linestyle="--", label=r"Melhor caso: $N$")
    ax.scatter([row["level"] for row in rows], [row["messages"] for row in rows], s=28, color="tab:red",
               alpha=0.5, linewidths=0, label="Execuções", zorder=2)
    groups = by_level(rows)
    stats = [mean_ci([row["messages"] for row in group]) for group in groups.values()]
    # A média é um risco do mesmo tamanho e espessura dos limites do IC95, e a barra tracejada
    # deixa visíveis as execuções sobrepostas a ela.
    cap = 5
    bars = ax.errorbar(list(groups), [mean for mean, _ in stats], yerr=[ci for _, ci in stats], color="darkred",
                       fmt="_", markersize=2 * cap, markeredgewidth=1.4, capsize=cap, capthick=1.4, elinewidth=1.4,
                       label="Média $\\pm$ IC95", zorder=3)
    bars[2][0].set_linestyle("--")
    ax.legend(loc="upper left", fontsize=9)
    ax.set_xlim(2.5, 7.5)
    ax.set_xticks([3, 5, 7])
    ax.set_xlabel("Número de nós ($N$)")
    ax.set_ylabel("Mensagens por eleição")
    ax.set_ylim(0, 70)
    ax.grid(alpha=0.35)
    fig.tight_layout()
    FIGURE.parent.mkdir(parents=True, exist_ok=True)
    for extension in ("pdf", "png"):
        fig.savefig(FIGURE.with_suffix(f".{extension}"), dpi=300)
    plt.close(fig)


def summarize_bully(rows: list[dict]) -> None:
    for n, group in by_level(rows).items():
        within = sum(n <= row["messages"] <= n * (n - 1) for row in group)
        print(f"  N={n:.0f}: {fmt(group, 'messages')}  {fmt(group, 'election_time_s', 0, 1000)} ms  "
              f"dentro de [N, N(N-1)]: {within}/{len(group)}")


def summarize_pb(rows: list[dict]) -> None:
    for n, group in by_level(rows).items():
        messages = sorted({row["messages_per_op"] for row in group})
        print(f"  N={n:.0f}: " + "  ".join(fmt(group, key) for key in (
            "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "throughput_ops_s",
            "heartbeats_per_s")) + f"  messages_per_op={messages}")


def summarize_hb(rows: list[dict]) -> None:
    for timeout, group in by_level(rows).items():
        print(f"  T={timeout}: {fmt(group, 'detection_s', 2)}  esperado [{timeout - HEARTBEAT_INTERVAL:.1f}; "
              f"{timeout + CHECK_INTERVAL:.1f}]  silence=[{min(row['silence_min_s'] for row in group):.2f}; "
              f"{max(row['silence_max_s'] for row in group):.2f}]  "
              f"false_suspicions={sum(row['false_suspicions'] for row in group):.0f}  "
              f"{fmt(group, 'heartbeats_per_node_s', 2)}")


def main() -> None:
    all_rows = []
    for experiment, summarize in (("bully", summarize_bully), ("pb", summarize_pb), ("hb", summarize_hb)):
        total, rows = load(experiment)
        durations = [row["duration_s"] for row in rows]
        print(f"{experiment}: {len(rows)}/{total} execuções corretas, duração de {min(durations):.0f} a "
              f"{max(durations):.0f} s; médias ± IC95:")
        summarize(rows)
        all_rows += rows
        if experiment == "bully":
            bully_figure(rows)
    startups = [row["startup_s"] for row in all_rows]
    kills = [row["kill_s"] for row in all_rows if "kill_s" in row]
    print(f"inicialização de {min(startups):.1f} a {max(startups):.1f} s; "
          f"encerramento do contêiner em {statistics.mean(kills):.2f} s em média; "
          f"tempo total de {sum(row['duration_s'] for row in all_rows) / 60:.0f} min")
    print(f"figura: {FIGURE}.pdf")


if __name__ == "__main__":
    main()
