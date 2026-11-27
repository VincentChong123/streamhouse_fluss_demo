# Preserve cached Docker images (do not pass --rmi)
docker compose \
    -f compose.yaml \
    -f /krunvm_configs/streamhouse_fluss_demo/compose.override.yaml \
    -f compose.otel.yaml \
    down --volumes
