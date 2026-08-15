#!/usr/bin/env bash
# 启动 TriggerLink 平台（Event API + Runner + Executor + Dashboard）
# 可用环境变量覆盖默认值，例如：
#   ADDR=:9000 EVENT_KEY=xxx SIGNING_KEY=yyy ./start.sh
set -euo pipefail
cd "$(dirname "$0")"

ADDR="${ADDR:-:8288}"
DB="${DB:-triggerlink.db}"
EVENT_KEY="${EVENT_KEY:-dev}"
SIGNING_KEY="${SIGNING_KEY:-dev}"
DASHBOARD_AUTH="${DASHBOARD_AUTH:-admin:admin}"
# 多个应用地址用空格分隔，如 APPS="http://localhost:8080/api/triggerlink http://localhost:8081/api/triggerlink"
APPS="${APPS:-}"

# 端口被占用时先杀掉占用进程再启动
PORT="${ADDR##*:}"
if pids=$(lsof -ti :"$PORT" 2>/dev/null); then
  echo "Port ${PORT} is in use (pids: $(echo $pids | tr '\n' ' ')), killing..."
  kill $pids 2>/dev/null || true
  for _ in $(seq 1 10); do
    lsof -ti :"$PORT" >/dev/null 2>&1 || break
    sleep 1
  done
  # 仍未退出则强杀
  if pids=$(lsof -ti :"$PORT" 2>/dev/null); then
    kill -9 $pids 2>/dev/null || true
    sleep 1
  fi
fi

args=(-addr "$ADDR" -db "$DB" -event-key "$EVENT_KEY" -signing-key "$SIGNING_KEY" -dashboard-auth "$DASHBOARD_AUTH")
for app in $APPS; do
  args+=(-app "$app")
done

echo "Starting TriggerLink on ${ADDR} (db=${DB})"
echo "Dashboard: http://localhost${ADDR}/dashboard (${DASHBOARD_AUTH%%:*} / ${DASHBOARD_AUTH#*:})"
exec go run ./cmd/triggerlink "${args[@]}"
