#!/bin/bash
set -eox pipefail

project="resgroup"
docker_compose_path="ci/docker-compose.yaml"
# Exit status file for cloud-init environments where exit codes aren't propagated.
# Parent processes can read this file to determine script success/failure.
logdir="$PWD/logs"
logfile=".exitcode"

function cleanup {
  # kill "$LOG_SYNC_PID" 2>/dev/null || true
  docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env down
}

mkdir ssh_keys "$logdir" -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

trap cleanup EXIT

#install gpdb and setup gpadmin user
bash ci/scripts/init_containers.sh $project cdw sdw1

# LOG_SYNC_INTERVAL=10
# LOG_ROOT="$PWD/logs"

# CDW_LOG_PATHS=(
#   "/home/gpadmin/gpAdminLogs"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/gpAdminLogs"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/standby/pg_log"
#   "/home/gpadmin/gpdb_src/src/test/isolation2/results/resgroup"
#   "/home/gpadmin/gpdb_src/src/test/isolation2/regression.diffs"
# )

# SDW1_LOG_PATHS=(
#   "/home/gpadmin/gpAdminLogs"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/gpAdminLogs"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror1/demoDataDir0/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1/pg_log"
#   "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2/pg_log"
# )

# copy_log_paths() {
#   local cid="$1"
#   local svc="$2"
#   shift 2
#   local paths=("$@")

#   local base="$LOG_ROOT/$svc"
#   mkdir -p "$base"

#   for src in "${paths[@]}"; do
#     dst="$base$src"
#     mkdir -p "$(dirname "$dst")"
#     docker cp "$cid:$src" "$dst" 2>/dev/null || true
#   done
# }

# sync_logs() {
#   while true; do
#     for svc in cdw sdw1; do
#       cid=$(docker compose -p "$project" ps -q "$svc" 2>/dev/null || true)
#       [ -z "$cid" ] && continue

#       case "$svc" in
#         cdw)
#           copy_log_paths "$cid" "$svc" "${CDW_LOG_PATHS[@]}"
#           ;;
#         sdw1)
#           copy_log_paths "$cid" "$svc" "${SDW1_LOG_PATHS[@]}"
#           ;;
#       esac
#     done
#     sleep "$LOG_SYNC_INTERVAL"
#   done
# }
# sync_logs &
# LOG_SYNC_PID=$!

for service in 'cdw' 'sdw1'
do
  #grant access rights to group controllers
  docker compose -p $project -f ci/docker-compose.yaml exec -T $service bash -c "
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset} &&
    mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb"
done

#create cluster
docker compose -p $project -f ci/docker-compose.yaml exec -T cdw \
 bash -c "source gpdb_src/concourse/scripts/common.bash && HOSTS_LIST='sdw1' make_cluster"

for service in 'cdw' 'sdw1'; do
 docker compose -p $project -f "$docker_compose_path" exec -T \
   $service bash -c "nohup /bin/bash gpdb_src/ci/scripts/collect_resgroup_logs.bash"
done

#disable exit on error to allow log collection regardless of return code
set +e
#run tests
docker compose -p $project -f ci/docker-compose.yaml exec -Tu gpadmin cdw bash -ex <<EOF
        source /usr/local/greengage-db-devel/greengage_path.sh
        source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        export LDFLAGS="-L\${GPHOME}/lib"
        export CPPFLAGS="-I\${GPHOME}/include"
        export USER=gpadmin

        cd /home/gpadmin/gpdb_src
        ./configure --prefix=/usr/local/greengage-db-devel \
            --without-zlib --without-rt --without-libcurl \
            --without-libedit-preferred --without-docdir --without-readline \
            --disable-gpcloud --disable-gpfdist --disable-orca \
            ${CONFIGURE_FLAGS}

        make -C /home/gpadmin/gpdb_src/src/test/regress
        ssh sdw1 mkdir -p /home/gpadmin/gpdb_src/src/test/{regress,isolation2} </dev/null
        scp /home/gpadmin/gpdb_src/src/test/regress/regress.so \
            gpadmin@sdw1:/home/gpadmin/gpdb_src/src/test/regress/

        make PGOPTIONS="-c optimizer=off" installcheck-resgroup || (
            errcode=\$?
            find src/test/isolation2 -name regression.diffs \
            | while read diff; do
                cat <<EOF1

======================================================================
DIFF FILE: \$diff
----------------------------------------------------------------------

EOF1
                cat \$diff
              done
            exit \$errcode
        )
EOF

#docker compose -p $project -f ci/docker-compose.yaml exec -T cdw bash -c '/bin/bash gpdb_src/ci/scripts/collect_resgroup_logs_once.bash'
#docker compose -p $project -f ci/docker-compose.yaml exec -T sdw1 bash -c '/bin/bash gpdb_src/ci/scripts/collect_resgroup_logs_once.bash'

# Cloud-init monitors will check for this file's existence and content.
# Missing file or invalid content will be interpreted as script failure.
exitcode=$?
echo "$exitcode" > "$logdir/$logfile"
exit "$exitcode"
