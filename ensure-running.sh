#!/bin/bash
# strict mode: fail fast instead of continuing after errors
set -euo pipefail
# log timestamp and line number when a command fails
trap 'rc=$?; [ $rc -ne 0 ] && echo "$(date "+%F %T") exited with $rc (line $LINENO)" >&2' EXIT

# move to the directory of this script, if that fails, exit with error code 1
cd /groups/umcg-as/tmp04/projects/paper-extracting/Repositories/emx2-hpc-bridge || exit 1

# already running Then do nothing.
if pgrep -u "$USER" -f "emx2_hpc_bridge\.main" > /dev/null; then
  exit 0
fi

# log that we are starting the bridge
echo "$(date) starting emx2-hpc-bridge" >> emx2_hpc_bridge.log

# start bridge in background, append all output to bridge.log
.venv/bin/python -u -m emx2_hpc_bridge.main >> emx2_hpc_bridge.log 2>&1 < /dev/null &
