#!/bin/bash
# 24/7 process supervisor. Not a Grok cron. Restarts the clipper if it exits.
cd /workspace/kalshi-desk
export PYTHONUNBUFFERED=1
while true; do
  echo "$(date -u +%H:%M:%S) supervisor start clipper" >> /workspace/kalshi-desk/mm.log
  /workspace/kalshi-desk/.venv/bin/python -u /workspace/kalshi-desk/ws_clip.py >> /workspace/kalshi-desk/mm.log 2>&1
  code=$?
  echo "$(date -u +%H:%M:%S) clipper exit $code; restart in 2s" >> /workspace/kalshi-desk/mm.log
  sleep 2
done
