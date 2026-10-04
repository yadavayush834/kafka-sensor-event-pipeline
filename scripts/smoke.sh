#!/bin/sh
# Run a real broker/producer/consumer round trip in CI or on a Docker host.
set -eu

docker compose up -d --build kafka init-topic consumer
docker compose run --rm producer --count 8 --interval 0

attempt=0
while [ "$attempt" -lt 30 ]; do
  report=$(docker compose exec -T consumer python -m sensor_pipeline.report --limit 8 2>/dev/null || true)
  printf '%s\n' "$report"
  if printf '%s\n' "$report" | grep -q 'total=8'; then
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep 2
done

docker compose logs consumer
exit 1
