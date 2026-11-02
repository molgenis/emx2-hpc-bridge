#!/bin/bash
cd /groups/umcg-as/tmp04/projects/paper-extracting/Repositories/emx2-hpc-bridge || exit 1

# Already running? Then do nothing.
if pgrep -f ".venv/bin/emx2-hpc-bridge" > /dev/null; then
  exit 0
fi

# If the bridge needs env vars/config, load them here, e.g.:
set -a; source .env; set +a

echo "$(date) starting emx2-hpc-bridge" >> bridge.log
export PYTHONUNBUFFERED=1
nohup .venv/bin/python -u -m emx2_hpc_bridge.main >> bridge.log 2>&1 &