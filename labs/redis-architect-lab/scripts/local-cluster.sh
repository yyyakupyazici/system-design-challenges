#!/usr/bin/env bash
# Yerel 6 düğümlü Redis Cluster (3 master + 3 replika), Docker gerekmez.
#   ./scripts/local-cluster.sh start   # 7000-7005 portlarında başlatır
#   ./scripts/local-cluster.sh stop
# Testler için: export IT_REDIS_CLUSTER=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
set -euo pipefail
BASE=${REDIS_LAB_DIR:-/tmp/redis-lab-cluster}
PORTS="7000 7001 7002 7003 7004 7005"

start() {
  for p in $PORTS; do
    mkdir -p "$BASE/$p"
    redis-server --port "$p" --cluster-enabled yes --cluster-config-file "$BASE/$p/nodes.conf" \
      --cluster-node-timeout 5000 --appendonly yes --dir "$BASE/$p" --daemonize yes \
      --logfile "$BASE/$p/redis.log" --save ""
  done
  for p in $PORTS; do until redis-cli -p "$p" ping >/dev/null 2>&1; do sleep 0.2; done; done
  if redis-cli -p 7000 cluster info | grep -q 'cluster_state:ok'; then
    echo "cluster zaten kurulu"; return
  fi
  redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
    127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1 --cluster-yes
  until redis-cli -p 7000 cluster info | grep -q 'cluster_state:ok'; do sleep 0.5; done
  echo "cluster hazır"
}

stop() {
  for p in $PORTS; do redis-cli -p "$p" shutdown nosave >/dev/null 2>&1 || true; done
  rm -rf "$BASE"
}

"${1:-start}"
