#!/bin/sh
# Collecte en continu ; compaction de la veille une fois par jour vers 01:20 UTC.
python collect.py --loop &
while true; do
  if [ "$(date -u +%H%M)" = "0120" ]; then
    python compact.py --purge-raw 30
    sleep 60
  fi
  sleep 30
done
