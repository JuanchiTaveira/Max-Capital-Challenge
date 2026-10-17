#!/usr/bin/env bash
set -euo pipefail

for topic in execution-reports settlements execution-reports-rejected; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 2 --replication-factor 1 \
    --config retention.ms=-1 --config retention.bytes=-1
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --describe --topic "$topic"
done
