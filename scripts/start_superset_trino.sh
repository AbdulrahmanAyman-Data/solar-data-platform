#!/usr/bin/env bash
set -Eeuo pipefail

echo "==> Starting Superset metadata database..."
docker compose up -d solar-postgres-superset

echo "==> Waiting for PostgreSQL to become healthy..."
for i in $(seq 1 60); do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' solar-postgres-superset 2>/dev/null || true)

    if [[ "$STATUS" == "healthy" ]]; then
        break
    fi

    if [[ "$i" -eq 60 ]]; then
        echo "ERROR: Superset PostgreSQL did not become healthy."
        docker compose logs --tail=60 solar-postgres-superset
        exit 1
    fi

    sleep 2
done

echo "==> Checking Superset metadata database..."
TABLE=$(docker compose exec -T solar-postgres-superset \
    psql -U superset -d superset \
    -tAc "SELECT to_regclass('public.ab_user');")

if [[ "$TABLE" != "ab_user" ]]; then
    echo "==> Initializing Superset..."
    docker compose run --rm solar-superset-init
else
    echo "==> Superset metadata database is already initialized."
fi

echo "==> Starting Superset..."
docker compose up -d solar-superset

echo
echo "==> Service status"
docker compose ps solar-superset solar-trino

echo
echo "Superset URL: http://localhost:8088"
echo "Trino connection URI for Superset:"
echo "trino://superset@solar-trino:8080/delta/solar"
