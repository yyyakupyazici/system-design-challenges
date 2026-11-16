#!/usr/bin/env bash
# Raft & etcd Lab: yerel "üç bölge".
#
#   etcd-ist  (İstanbul)  client 23791  peer 23801
#   etcd-ank  (Ankara)    client 23792  peer 23802
#   etcd-wit  (tanık)     client 23793  peer 23803     ← müşteri verisi yok, sadece oy
#
#   pg-ist    (İstanbul)  5511  primary
#   pg-ank    (Ankara)    5512  standby (senkron adayı)
#   pg-ist2   (İstanbul)  5513  standby
#   pg-ank2   (Ankara)    5514  standby  ← İstanbul komple giderse Ankara'da ikinci kopya (RPO 0 failover'dan sonra da sürsün)
#
# Kullanım:
#   lab.sh up | down | status
#   lab.sh etcd-up | etcd-down | pg-up | pg-down
#   lab.sh kill  <etcd-ist|etcd-ank|etcd-wit|pg-ist|pg-ank|pg-ist2|pg-ank2>    süreci öldür (bina yandı)
#   lab.sh pause <...> / resume <...>                                  süreci dondur / çöz (ağ koptu gibi)
#   lab.sh start <...>                                                 tek bir süreci yeniden başlat
set -euo pipefail

LAB_DIR="${LAB_DIR:-/var/tmp/raftlab}"
PG_BIN="${PG_BIN:-$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)}"
PG_USER="${PG_USER:-postgres}"
ETCD_BIN="${ETCD_BIN:-$(command -v etcd || true)}"

declare -A ETCD_CLIENT=([etcd-ist]=23791 [etcd-ank]=23792 [etcd-wit]=23793)
declare -A ETCD_PEER=([etcd-ist]=23801 [etcd-ank]=23802 [etcd-wit]=23803)
declare -A PG_PORT=([pg-ist]=5511 [pg-ank]=5512 [pg-ist2]=5513 [pg-ank2]=5514)

ETCD_NODES="etcd-ist etcd-ank etcd-wit"
PG_NODES="pg-ist pg-ank pg-ist2 pg-ank2"
CLUSTER="etcd-ist=http://127.0.0.1:23801,etcd-ank=http://127.0.0.1:23802,etcd-wit=http://127.0.0.1:23803"

ENDPOINTS="http://127.0.0.1:23791,http://127.0.0.1:23792,http://127.0.0.1:23793"

as_pg() {
  if [ "$(id -un)" = "$PG_USER" ]; then bash -c "$1"; else su "$PG_USER" -s /bin/bash -c "$1"; fi
}

pidfile() { echo "$LAB_DIR/run/$1.pid"; }

alive() {
  local f; f=$(pidfile "$1")
  [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null
}

# ------------------------------------------------------------------ etcd

etcd_start_one() {
  local n=$1 state=${2:-existing}
  if alive "$n"; then return; fi
  mkdir -p "$LAB_DIR/etcd/$n" "$LAB_DIR/run" "$LAB_DIR/log"
  # Kısa zaman aşımları: lider seçimi saniyeler değil ~1 sn sürsün (gerçek şehirler arası kurulumda RTT'ye göre ayarlanır)
  nohup "$ETCD_BIN" --name "$n" --data-dir "$LAB_DIR/etcd/$n" \
    --listen-client-urls "http://127.0.0.1:${ETCD_CLIENT[$n]}" --advertise-client-urls "http://127.0.0.1:${ETCD_CLIENT[$n]}" \
    --listen-peer-urls "http://127.0.0.1:${ETCD_PEER[$n]}" --initial-advertise-peer-urls "http://127.0.0.1:${ETCD_PEER[$n]}" \
    --initial-cluster "$CLUSTER" --initial-cluster-token raftlab --initial-cluster-state "$state" \
    --heartbeat-interval 100 --election-timeout 1000 \
    > "$LAB_DIR/log/$n.log" 2>&1 &
  echo $! > "$(pidfile "$n")"
}

etcd_up() {
  local state=new
  [ -d "$LAB_DIR/etcd/etcd-ist/member" ] && state=existing
  for n in $ETCD_NODES; do etcd_start_one "$n" "$state"; done
  for _ in $(seq 1 50); do
    if ETCDCTL_API=3 etcdctl --endpoints "$ENDPOINTS" endpoint health >/dev/null 2>&1; then echo "etcd: 3 düğüm ayakta"; return; fi
    sleep 0.2
  done
  echo "etcd ayağa kalkmadı, loglar: $LAB_DIR/log" >&2; exit 1
}

# ------------------------------------------------------------------ PostgreSQL

pg_conf() {
  local n=$1
  cat <<CONF
port = ${PG_PORT[$n]}
listen_addresses = '127.0.0.1'
unix_socket_directories = '$LAB_DIR/sock'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
# Standby geride kalınca ihtiyaç duyduğu WAL silinmesin (yoksa: "requested WAL segment ... has already been removed").
# Üretimde bunun yerine replication slot kullanılır (Patroni her üye için slot açar).
wal_keep_size = 256MB
# Cevap vermeyen standby 5 sn sonra düşürülür (varsayılan 60 sn). Senkron standby donarsa ajan başka birini seçebilsin.
wal_sender_timeout = 5s
hot_standby = on
hot_standby_feedback = on
wal_log_hints = on
synchronous_commit = on
# Senkron standby'ı lab ajanı (mini-Patroni) yönetir: ALTER SYSTEM SET synchronous_standby_names
wal_receiver_status_interval = 1s
wal_retrieve_retry_interval = 500ms
shared_buffers = 32MB
max_connections = 40
fsync = on
log_line_prefix = '%m [$n] '
CONF
}

pg_init() {
  [ -f "$LAB_DIR/pg/pg-ist/PG_VERSION" ] && return
  mkdir -p "$LAB_DIR/pg" "$LAB_DIR/sock" "$LAB_DIR/log" "$LAB_DIR/run"
  chown -R "$PG_USER" "$LAB_DIR/pg" "$LAB_DIR/sock" "$LAB_DIR/log"
  as_pg "$PG_BIN/initdb -D $LAB_DIR/pg/pg-ist -A trust -U postgres --no-instructions >/dev/null 2>&1"
  pg_conf pg-ist | as_pg "cat >> $LAB_DIR/pg/pg-ist/postgresql.conf"
  echo "host replication all 127.0.0.1/32 trust" | as_pg "cat >> $LAB_DIR/pg/pg-ist/pg_hba.conf"
  pg_start_one pg-ist
  as_pg "$PG_BIN/psql -q -h 127.0.0.1 -p 5511 -U postgres -c 'CREATE DATABASE ledger'"
  as_pg "$PG_BIN/psql -q -h 127.0.0.1 -p 5511 -U postgres -d ledger" <<'SQL'
CREATE TABLE accounts (
    id      text PRIMARY KEY,
    balance bigint NOT NULL CHECK (balance >= 0)        -- kuruş
);
CREATE TABLE transfers (
    id         bigserial PRIMARY KEY,
    from_acc   text   NOT NULL,
    to_acc     text   NOT NULL,
    amount     bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
SQL
  for n in pg-ank pg-ist2 pg-ank2; do
    as_pg "$PG_BIN/pg_basebackup -h 127.0.0.1 -p 5511 -U postgres -D $LAB_DIR/pg/$n -R -X stream -c fast"
    # standby'ın adı = application_name: synchronous_standby_names bu adla eşleşir
    as_pg "sed -i \"s/^primary_conninfo = '/primary_conninfo = 'application_name=$n /\" $LAB_DIR/pg/$n/postgresql.auto.conf"
    as_pg "sed -i 's/^port = .*/port = ${PG_PORT[$n]}/; s/\[pg-ist\]/[$n]/' $LAB_DIR/pg/$n/postgresql.conf"
  done
}

pg_start_one() {
  local n=$1
  if as_pg "$PG_BIN/pg_ctl -D $LAB_DIR/pg/$n status" >/dev/null 2>&1; then return; fi
  rm -f "$LAB_DIR/pg/$n/postmaster.pid"
  as_pg "$PG_BIN/pg_ctl -D $LAB_DIR/pg/$n -l $LAB_DIR/log/$n.log -w -t 30 start" >/dev/null
  head -1 "$LAB_DIR/pg/$n/postmaster.pid" > "$(pidfile "$n")"
}

pg_up() {
  pg_init
  for n in $PG_NODES; do pg_start_one "$n"; done
  echo "pg: pg-ist(5511) pg-ank(5512) pg-ist2(5513) pg-ank2(5514) ayakta"
}

pg_stop_one() {
  as_pg "$PG_BIN/pg_ctl -D $LAB_DIR/pg/$1 -m immediate stop" >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------ durum

status() {
  echo "── etcd (Raft) ─────────────────────────────────────────────────"
  ETCDCTL_API=3 etcdctl --endpoints "$ENDPOINTS" --command-timeout=1s endpoint status -w table 2>/dev/null \
    || echo "(bazı düğümlere ulaşılamadı)"
  echo "── /paramix kayıtları ──────────────────────────────────────────"
  ETCDCTL_API=3 etcdctl --endpoints "$ENDPOINTS" --command-timeout=1s get --prefix /paramix 2>/dev/null | paste - - || true
  echo "── PostgreSQL ──────────────────────────────────────────────────"
  for n in $PG_NODES; do
    printf '%-8s ' "$n"
    as_pg "timeout 3 $PG_BIN/psql -X -A -t -h 127.0.0.1 -p ${PG_PORT[$n]} -U postgres -d postgres -c \"select case when pg_is_in_recovery() then 'standby  replay=' || coalesce(pg_last_wal_replay_lsn()::text,'-') else 'PRIMARY  lsn=' || pg_current_wal_lsn()::text || '  sync=' || current_setting('synchronous_standby_names') end || '  read_only=' || current_setting('default_transaction_read_only')\"" 2>/dev/null \
      || echo "kapalı / cevap vermiyor"
  done
}

# ------------------------------------------------------------------ süreç kontrolü

signal() {
  local sig=$1 n=$2 f; f=$(pidfile "$n")
  [ -f "$f" ] || { echo "$n çalışmıyor" >&2; return 0; }
  if [ "$sig" = KILL ] && [[ $n == pg-* ]]; then
    kill -KILL "$(cat "$f")" 2>/dev/null || true       # postmaster öldü: çocuklar da kendini kapatır
    pkill -KILL -f "postgres -D $LAB_DIR/pg/$n" 2>/dev/null || true
    pkill -KILL -f -- "$LAB_DIR/pg/$n" 2>/dev/null || true
    rm -f "$LAB_DIR/pg/$n/postmaster.pid"
  elif [[ $n == pg-* ]]; then
    # bütün PostgreSQL süreçlerini (postmaster + çocukları) dondur/çöz
    pkill "-$sig" -P "$(cat "$f")" 2>/dev/null || true
    kill "-$sig" "$(cat "$f")" 2>/dev/null || true
  else
    kill "-$sig" "$(cat "$f")" 2>/dev/null || true
  fi
}

start_one() {
  case $1 in
    etcd-*) etcd_start_one "$1" existing ;;
    pg-*) pg_start_one "$1" ;;
  esac
}

cmd=${1:-status}
case $cmd in
  up) etcd_up; pg_up ;;
  etcd-up) etcd_up ;;
  pg-up) pg_up ;;
  etcd-down) for n in $ETCD_NODES; do signal KILL "$n"; done; rm -f "$LAB_DIR"/run/etcd-*.pid ;;
  pg-down) for n in $PG_NODES; do pg_stop_one "$n"; done ;;
  down) for n in $ETCD_NODES; do signal KILL "$n"; done; for n in $PG_NODES; do pg_stop_one "$n"; done; rm -f "$LAB_DIR"/run/*.pid ;;
  clean) "$0" down || true; rm -rf "${LAB_DIR:?}" ;;
  status) status ;;
  kill) signal KILL "$2" ;;
  pause) signal STOP "$2" ;;
  resume) signal CONT "$2" ;;
  start) start_one "$2" ;;
  endpoints) echo "$ENDPOINTS" ;;
  *) echo "bilinmeyen komut: $cmd" >&2; exit 2 ;;
esac
