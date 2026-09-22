#!/bin/sh
# Run all carbon unit tests manually (mirrors the tox testenv setup).
#
# Usage:
#   ./test.sh                        # run the full unit test suite
#   ./test.sh carbon.tests.test_client   # run a single test module
#
# Requires: twisted, mock (see requirements.txt / tests-requirements.txt).

set -e

cd "$(dirname "$0")"

export GRAPHITE_NO_PREFIX=true
export PYTHONPATH="$(pwd)/lib${PYTHONPATH:+:$PYTHONPATH}"

if command -v trial >/dev/null 2>&1; then
  TRIAL=trial
else
  TRIAL="python3 -m twisted.trial"
fi

if [ $# -gt 0 ]; then
  # Run only the given test module(s)/case(s).
  exec $TRIAL "$@"
fi

# Run every unit test module under lib/carbon/tests.
exec $TRIAL carbon
