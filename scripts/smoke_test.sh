#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'EOF'
Run smoke tests for the Docker test harness.

Usage:
  ./scripts/smoke_test.sh [behave args...]

Examples:
  ./scripts/smoke_test.sh
  ./scripts/smoke_test.sh --tags=@smoke

Behavior:
  - Host environment: force Docker image rebuild, then run Behave @smoke tests in container.
  - Inside Docker: run Behave @smoke tests directly (no nested Docker needed).
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ "${AIR_QUALITY_TEST_IN_DOCKER:-}" == "1" || -f /.dockerenv ]]; then
    exec "$SCRIPT_DIR/run_behave_tests.sh" --inside --tags=@smoke "$@"
fi

# Host mode: force a Docker build, then run smoke tests inside the image.
exec "$SCRIPT_DIR/run_behave_tests.sh" --build -- --tags=@smoke "$@"
