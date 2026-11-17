"""Compara duas execuções da malha de experimentos (máquinas diferentes).

Para cada cenário presente nas duas pastas e cada nível, compara as métricas principais:
média ± IC95 em A e B, diferença relativa (B-A)/A e teste t de Welch (|t| > t_crit → diferença
significativa a 95%). Também lista diferenças de configuração/ambiente entre as execuções.

Saídas em <out>/:
  compare.csv                 uma linha por (cenário, nível, métrica)
  figures/compare_<exp>_*.pdf|png  as duas máquinas no mesmo gráfico

Uso: python experiments/compare_results.py [--a results/raw] [--b results_02/results/raw]
"""
import json
import math
from argparse import ArgumentParser
from collections import Counter
from pathlib import Path

from analyze_results import LABELS, aggregate, line_chart, load, scalar_rows, t_crit, write_csv

METRICS = {
    "bully": ("ok", "detection_first_s", "election_time_s", "recovery_time_s", "failover_time_s",
              "election_rounds", "bully_messages"),
    "pb": ("ok", "throughput_ops_s", "latency_ms_mean", "latency_ms_p50", "latency_ms_p95",
           "latency_ms_p99", "messages_per_op"),
    "hb": ("ok", "detection_first_s", "detection_last_s", "false_suspicions_per_min",
           "heartbeats_per_node_s", "failover_time_s"),
    "backup": ("ok", "detection_leader_s", "reconfiguration_time_s", "failover_time_s",
               "reconfiguration_messages", "election_rounds"),
}
# (métrica, rótulo do eixo y) dos gráficos comparativos
PLOTS = {
    "bully": [("detection_first_s", "Detecção (s)"), ("election_time_s", "Eleição (s)"),
              ("recovery_time_s", "Recuperação (s)"), ("failover_time_s", "Falha → PRIMARY (s)")],
    "pb": [("latency_ms_mean", "Latência média (ms)"), ("latency_ms_p99", "Latência p99 (ms)"),
           ("throughput_ops_s", "Vazão (escritas/s)")],
    "hb": [("detection_first_s", "Detecção (s)"), ("false_suspicions_per_min", "Falsas suspeitas/min"),
           ("failover_time_s", "Falha → PRIMARY (s)")],
    "backup": [("detection_leader_s", "Detecção pelo líder (s)"), ("reconfiguration_time_s", "Reconfiguração (s)"),
               ("failover_time_s", "Falha → PRIMARY (s)")],
}
CONFIG_KEYS = ("ops", "warmup_ops", "pre_tasks", "window_s", "load_rate", "heartbeat_interval",
               "failure_timeout", "request_timeout", "git_commit")
ENV_KEYS = ("cpu_model", "cpu_count", "memory_bytes", "os", "docker_server", "docker_compose", "image_python")


def welch(a: dict, b: dict) -> tuple[float | None, float | None]:
    """Estatística t de Welch e valor crítico (95%, g.l. de Welch-Satterthwaite)."""
    va, vb = a["std"] ** 2 / a["n"], b["std"] ** 2 / b["n"]
    if va + vb == 0:
        return None, None
    t = (b["mean"] - a["mean"]) / math.sqrt(va + vb)
    dof = (va + vb) ** 2 / ((va ** 2 / (a["n"] - 1) if va else 0) + (vb ** 2 / (b["n"] - 1) if vb else 0))
    return t, t_crit(max(1, round(dof)))


def latest_env(raw: Path) -> dict:
    files = sorted(raw.glob("environment_*.json"))
    return json.loads(files[-1].read_text()) if files else {}


def config_summary(rows: list[dict]) -> str:
    # window_s do hb é a janela medida (varia em ms): arredonda
    counts = Counter(tuple(round(v, 1) if isinstance(v, float) else v for v in (r.get(k) for k in CONFIG_KEYS))
                     for r in rows)
    return "; ".join(f"{n}× " + ", ".join(f"{k}={str(v)[:7]}" for k, v in zip(CONFIG_KEYS, cfg))
                     for cfg, n in counts.most_common())


def main(args):
    raw_a, raw_b = Path(args.a), Path(args.b)
    out = Path(args.out)
    figures = out / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    env_a, env_b = latest_env(raw_a), latest_env(raw_b)
    print("== Ambiente (A | B)")
    for key in ENV_KEYS:
        mark = "" if env_a.get(key) == env_b.get(key) else "  *"
        print(f"{key}: {env_a.get(key)} | {env_b.get(key)}{mark}")

    folders = sorted({p.name for p in raw_a.iterdir() if p.is_dir()} & {p.name for p in raw_b.iterdir() if p.is_dir()})
    only = sorted({p.name for p in raw_a.iterdir() if p.is_dir()} ^ {p.name for p in raw_b.iterdir() if p.is_dir()})
    if only:
        print(f"\nCenários em apenas uma das execuções (ignorados): {', '.join(only)}")

    rows_out = []
    for folder in folders:
        exp = folder.split("_")[0]
        runs_a, runs_b = load(raw_a, folder), load(raw_b, folder)
        rows_a, rows_b = scalar_rows(runs_a), scalar_rows(runs_b)
        agg_a, agg_b = aggregate(runs_a, rows_a, exp), aggregate(runs_b, rows_b, exp)
        param = rows_a[0]["param"]

        print(f"\n== {folder}  (A: {len(runs_a)} réplicas, B: {len(runs_b)} réplicas)")
        print(f"config A: {config_summary(rows_a)}")
        print(f"config B: {config_summary(rows_b)}")
        print(f"{'nível':>6} {'métrica':<26} {'A (média ± IC95)':>22} {'B (média ± IC95)':>22} {'Δ%':>8} {'t':>7}  sig.")
        for level in sorted(set(agg_a) & set(agg_b)):
            for key in METRICS[exp]:
                a, b = agg_a[level].get(key), agg_b[level].get(key)
                if not a or not b or a.get("mean") is None or b.get("mean") is None:
                    continue
                t, tc = welch(a, b)
                rel = (b["mean"] - a["mean"]) / a["mean"] * 100 if a["mean"] else None
                sig = t is not None and abs(t) > tc
                rows_out.append({"experiment": folder, "param": param, "level": level, "metric": key,
                                 "n_a": a["n"], "mean_a": a["mean"], "ci95_a": a["ci95"],
                                 "n_b": b["n"], "mean_b": b["mean"], "ci95_b": b["ci95"],
                                 "diff": b["mean"] - a["mean"], "diff_pct": rel,
                                 "welch_t": t, "t_crit": tc, "significant": sig})
                rel_s = f"{rel:+.1f}" if rel is not None else "NA"
                t_s = f"{t:+.2f}" if t is not None else "NA"
                print(f"{level:>6} {key:<26} {a['mean']:>12.4f} ± {a['ci95']:<7.4f} "
                      f"{b['mean']:>12.4f} ± {b['ci95']:<7.4f} {rel_s:>8} {t_s:>7}  {'*' if sig else ''}")

        # gráficos: as duas execuções como séries do mesmo gráfico
        merged = {lvl: {**{f"A:{k}": v for k, v in agg_a[lvl].items()}, **{f"B:{k}": v for k, v in agg_b[lvl].items()}}
                  for lvl in sorted(set(agg_a) & set(agg_b))}
        for key, ylabel in PLOTS[exp]:
            line_chart(figures / f"compare_{folder}_{key}", merged,
                       [(f"A:{key}", args.label_a), (f"B:{key}", args.label_b)],
                       LABELS.get(param, param), ylabel, f"{folder}: {key}")

    write_csv(out / "compare.csv", rows_out)
    n_sig = sum(r["significant"] for r in rows_out)
    print(f"\n{n_sig}/{len(rows_out)} comparações com diferença significativa (Welch, 95%). "
          f"Saídas: {out / 'compare.csv'}, {figures}/compare_*")


if __name__ == "__main__":
    here = Path(__file__).parent
    parser = ArgumentParser(description="Comparação entre duas execuções dos experimentos")
    parser.add_argument("--a", default=str(here / "results" / "raw"), help="raw da execução A")
    parser.add_argument("--b", default=str(here / "results_02" / "results" / "raw"), help="raw da execução B")
    parser.add_argument("--label-a", default="Máquina A (Ryzen 7 5700X, WSL2)")
    parser.add_argument("--label-b", default="Máquina B (i7-1165G7, Ubuntu)")
    parser.add_argument("--out", default=str(here / "results" / "compare"))
    main(parser.parse_args())
