"""
clean_stress_metrics.py
=======================
Delete stale stress-test rows from `performance_metrics` that have
`avg_latency_ms = 0` — these are artifacts written BEFORE the stress test
started recording real end-to-end latency. Removing them keeps the dashboard's
"Stress Test Metrics" latency chart showing only real measurements.

Safe + scoped:
  - Only touches mode='stress' rows whose avg_latency_ms is 0 / null.
  - Prints each row it removes; never deletes real (latency > 0) rows.
  - Set DRY_RUN=1 to preview without deleting.

Usage:
    CASSANDRA_HOST=127.0.0.1 python infrastructure/clean_stress_metrics.py
    DRY_RUN=1 CASSANDRA_HOST=127.0.0.1 python infrastructure/clean_stress_metrics.py
"""

import os

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    rows = list(session.execute(
        "SELECT timestamp, metric_id, avg_latency_ms, messages_per_second, notes "
        "FROM performance_metrics WHERE mode='stress'"
    ))
    stale = [r for r in rows if not r.avg_latency_ms]  # 0 or None

    print(f"Found {len(rows)} stress rows; {len(stale)} stale (avg_latency_ms=0).")
    if not stale:
        print("Nothing to clean.")
        cluster.shutdown()
        return

    delete = session.prepare(
        "DELETE FROM performance_metrics "
        "WHERE mode='stress' AND timestamp=? AND metric_id=?"
    )
    removed = 0
    for r in stale:
        print(f"  {'[dry-run] would remove' if DRY_RUN else 'removing'}: "
              f"{r.timestamp}  ~{r.messages_per_second} msg/s  ({r.notes})")
        if not DRY_RUN:
            session.execute(delete, (r.timestamp, r.metric_id))
            removed += 1

    print("-" * 50)
    if DRY_RUN:
        print(f"DRY_RUN: {len(stale)} rows would be removed (none deleted).")
    else:
        print(f"Removed {removed} stale stress rows. The latency chart now shows "
              "only real measurements.")
    cluster.shutdown()


if __name__ == "__main__":
    main()
