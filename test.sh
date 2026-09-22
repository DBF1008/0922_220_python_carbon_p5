#!/usr/bin/env bash
#
# Manual unit-test runner for the carbon relay client changes.
#
# Usage:
#   ./test.sh                 # run the full carbon unit suite via trial
#   ./test.sh client          # run only the client tests
#   ./test.sh <trial.path>    # run any trial target, e.g.
#                             #   carbon.tests.test_client.PooledReplicaSelectionTest
#
# Environment overrides:
#   PYTHON_BIN  Python interpreter to use (default: python3 on PATH)
#   TRIAL_ARGS  Extra arguments forwarded to trial

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
export GRAPHITE_NO_PREFIX=true
export PYTHONPATH="$SCRIPT_DIR/lib${PYTHONPATH:+:$PYTHONPATH}"

if ! "$PYTHON_BIN" -c "import twisted" >/dev/null 2>&1; then
  echo "error: Twisted is not importable by '$PYTHON_BIN'." >&2
  echo "Install test dependencies first, e.g.:" >&2
  echo "  $PYTHON_BIN -m pip install -r requirements.txt -r tests-requirements.txt" >&2
  exit 1
fi

target="${1:-carbon}"
case "$target" in
  client)
    trial_target="carbon.tests.test_client"
    ;;
  *)
    trial_target="$target"
    ;;
esac

echo "==> Running: $trial_target"
echo "    interpreter: $("$PYTHON_BIN" --version 2>&1)"
echo

"$PYTHON_BIN" -m twisted.trial ${TRIAL_ARGS:-} "$trial_target"
