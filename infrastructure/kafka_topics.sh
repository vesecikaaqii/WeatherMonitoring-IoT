#!/usr/bin/env bash
# ==========================================================================
# kafka_topics.sh
# Create the Kafka topics used by the Weather Monitoring IoT pipeline.
#
# Topics:
#   weather_data         (>= 3 partitions)  raw sensor readings
#   weather_alerts       (>= 3 partitions)  alarm events
#   performance_metrics  (1  partition )    throughput / latency samples
#
# Bootstrap server is read from the environment so the same script works on a
# laptop and inside Docker.
#
# Usage (host):
#   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 bash infrastructure/kafka_topics.sh
# Usage (inside the kafka container):
#   docker exec -it kafka bash /scripts/kafka_topics.sh
# ==========================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

# kafka-topics.sh location differs between distributions; try both.
if command -v kafka-topics.sh >/dev/null 2>&1; then
  KT="kafka-topics.sh"
elif command -v kafka-topics >/dev/null 2>&1; then
  KT="kafka-topics"
else
  KT="/opt/kafka/bin/kafka-topics.sh"
fi

create_topic () {
  local name="$1"; local parts="$2"
  echo "Creating topic '${name}' with ${parts} partition(s)..."
  "$KT" --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$name" \
        --partitions "$parts" \
        --replication-factor 1
}

create_topic "weather_data"        3
create_topic "weather_alerts"      3
create_topic "performance_metrics" 1

echo "Done. Current topics:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
