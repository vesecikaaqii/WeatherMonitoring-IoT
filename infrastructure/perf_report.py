"""
perf_report.py
==============
Generate REAL performance-table numbers for the README straight from Cassandra
(`performance_metrics`) — no hand-typed / invented values.

Run it AFTER the pipeline has produced data and you have run one or more stress
tests, then paste the printed markdown into the README "Performance metrics &
optimization" section.

    CASSANDRA_HOST=127.0.0.1 python infrastructure/perf_report.py

How the columns are sourced (so the table is honest):
  - `stream` rows  : written by Spark per micro-batch (live pipeline) — these
                     carry the real avg/max latency and Cassandra write time,
                     INCLUDING the peaks recorded while a stress test runs.
  - `stress` rows  : one summary row per stress test. Achieved msg/s plus the
                     REAL avg/max end-to-end latency, which the harness reads
                     back from the `stream` metrics Spark wrote during the burst
                     (the producer itself can't see pipeline latency). The target
                     throughput is stored in `notes` as `target=<N>`. Cassandra
                     write time is not recorded per stress summary (shown from
                     the `stream` samples instead).
"""

import os
import re
from collections import defaultdict

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 1) if xs else 0.0


def _max(xs):
    xs = [x for x in xs if x is not None]
    return round(max(xs), 1) if xs else 0.0


def _fetch(session, mode):
    return list(session.execute(
        "SELECT messages_processed, messages_per_second, avg_latency_ms, "
        "max_latency_ms, cassandra_write_time_ms, notes "
        f"FROM performance_metrics WHERE mode='{mode}'"
    ))


def _parse_target(notes):
    if not notes:
        return None
    m = re.search(r"target=(\d+)", notes)
    return int(m.group(1)) if m else None


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    stream = _fetch(session, "stream")
    normal = _fetch(session, "normal")
    stress = _fetch(session, "stress")

    print("### Measured results\n")
    print("| Mode | Target msg/s | Achieved | Avg latency | Max latency | Cassandra write |")
    print("|------|-------------|----------|-------------|-------------|-----------------|")

    # normal + stream: one aggregated row each (live Spark pipeline metrics)
    for label, rows in (("normal", normal), ("stream", stream)):
        if not rows:
            continue
        print(f"| {label} | — | ~{_avg([r.messages_per_second for r in rows])} "
              f"| {_avg([r.avg_latency_ms for r in rows])} ms "
              f"| {_max([r.max_latency_ms for r in rows])} ms "
              f"| {_avg([r.cassandra_write_time_ms for r in rows])} ms |")

    # stress: grouped by target throughput
    groups = defaultdict(list)
    for r in stress:
        groups[_parse_target(r.notes)].append(r)
    for target in sorted(groups, key=lambda t: (t is None, t or 0)):
        rows = groups[target]
        tcol = target if target is not None else "—"
        # avg/max latency now come from the stress row itself (real, measured
        # from stream during the burst); Cassandra write time from stream samples.
        print(f"| stress | {tcol} | ~{_avg([r.messages_per_second for r in rows])} "
              f"| {_avg([r.avg_latency_ms for r in rows])} ms "
              f"| {_max([r.max_latency_ms for r in rows])} ms "
              f"| {_avg([r.cassandra_write_time_ms for r in stream]) if stream else 0.0} ms |")

    print()
    print(f"<!-- samples: stream={len(stream)} normal={len(normal)} "
          f"stress={len(stress)} -->")
    if not (stream or normal or stress):
        print("\nNo performance_metrics rows found. Run the Spark job + a stress "
              "test first:\n"
              "  Simulator Control -> Run stress test  (or POST /stress-test)")

    cluster.shutdown()


if __name__ == "__main__":
    main()
