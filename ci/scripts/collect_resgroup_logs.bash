#!/bin/bash
set -eox pipefail

echo "[log-sync] started on $(hostname) at $(date)" >&2

LOG_SYNC_INTERVAL=10
HOST_LOG_DIR="/logs/$(hostname)"

mkdir -p /logs/$(hostname)
cat > diffs.list <<EOF
results/resgroup/
regression.diffs
EOF
cat > logs.list <<EOF
gpAdminLogs/
standby/pg_log/
qddir/demoDataDir-1/pg_log/
dbfast1/demoDataDir0/pg_log/
dbfast2/demoDataDir1/pg_log/
dbfast3/demoDataDir2/pg_log/
dbfast_mirror1/demoDataDir0/pg_log/
dbfast_mirror2/demoDataDir1/pg_log/
dbfast_mirror3/demoDataDir2/pg_log/
EOF

# check for directory exists
sync_dir() {
  local src="$1"
  local dst="$2"
  local files_from="$3"

  if [ -n "$files_from" ]; then
    [ -d "$src" ] && rsync -a --ignore-missing-args --whole-file --inplace --files-from="$files_from" "$src" "$dst" || true
  else
    [ -d "$src" ] && rsync -a --ignore-missing-args --whole-file --inplace "$src" "$dst" || true
  fi
}

# sync gpAdminLogs
while true; do
  sync_dir "/home/gpadmin/gpAdminLogs" "$HOST_LOG_DIR"
  sleep $LOG_SYNC_INTERVAL
done &

# sync gpdb_src/gpAux/gpdemo/datadirs
while true; do
  sync_dir "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs" "$HOST_LOG_DIR" "logs.list"
  sleep $LOG_SYNC_INTERVAL
done &

# sync diffs
while true; do
  sync_dir "/home/gpadmin/gpdb_src/src/test/isolation2" "$HOST_LOG_DIR" "diffs.list"
  sleep $LOG_SYNC_INTERVAL
done &
