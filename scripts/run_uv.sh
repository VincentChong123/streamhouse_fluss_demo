#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export UV_PROJECT_ENVIRONMENT="$HOME/.cache/virtualenv/streamhouse_fluss_demo-notebooks"

pushd "$SCRIPT_DIR/../notebooks" > /dev/null
    uv run --offline marimo run . "$@" || uv run marimo run . "$@"
popd > /dev/null
