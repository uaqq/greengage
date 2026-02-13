#!/bin/bash
set -eox pipefail

echo "[log-sync] started on $(hostname) at $(date)" >&2

HOST_LOG_DIR="/logs"
BASE_DIR="/home/gpadmin"
GPDB_SRC_DIR="gpdb_src"

mkdir -p $HOST_LOG_DIR

LOGS_DIR=(
  "${BASE_DIR}/gpAdminLogs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/gpAdminLogs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/standby/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/src/test/isolation2/results/resgroup"
  "${BASE_DIR}/${GPDB_SRC_DIR}/src/test/isolation2/regression.diffs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror1/demoDataDir0/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2/pg_log"
)

sync_dir() {
  local src="$1"
  local dst="$2"
  if [ -d "$src" ]; then
    mkdir -p "$dst"
    rsync -a --ignore-missing-args --whole-file --inplace "$src/" "$dst/" || true
  elif [ -f "$src" ]; then
    mkdir -p "$(dirname "$dst")"
    rsync -a --ignore-missing-args --whole-file --inplace "$src" "$dst"
  fi
}

for src in "${LOGS_DIR[@]}"; do
  rel=${src#"$BASE_DIR/"}
  dst="$HOST_LOG_DIR/$rel"
  sync_dir "$src" "$dst" &
done
