#!/bin/bash
# 启动 Trade_db 服务
# Usage: ./run.sh
#
# 优先使用 systemd 管理（已安装 trade_db.service 时自动代理）；
# 未安装 systemd 服务时退回 nohup 手工启动。

cd "$(dirname "$0")"
mkdir -p logs

PROC_REGEX="[p]ython3?.*Trade_db/main\.py"
PIDFILE="logs/trade_db.pid"

# ── systemd 路径 ──────────────────────────────────────────────
if systemctl list-unit-files 2>/dev/null | grep -q ^trade_db.service; then
    STATUS=$(systemctl is-active trade_db.service 2>/dev/null)
    if [ "$STATUS" = "active" ]; then
        echo "ℹ️  Trade_db 已在运行（systemd）。如需重启请执行 ./restart.sh 或:"
        echo "   systemctl restart trade_db.service"
        exit 0
    fi
    echo "🚀 正在启动 trade_db.service (systemd)..."
    systemctl start trade_db.service
    sleep 2
    if systemctl is-active --quiet trade_db.service; then
        echo "✅ Trade_db 启动成功（systemd）"
        echo "💡 查看日志: journalctl -u trade_db.service -f"
        echo "   或:       tail -f trade_db.log"
    else
        echo "❌ 启动失败，查看日志:"
        echo "   journalctl -u trade_db.service -n 30 --no-pager"
        exit 1
    fi
    exit 0
fi

# ── 手工 nohup 路径（未安装 systemd 服务时使用）──────────────
# 清理失效 pidfile
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ! ps -p "$PID" > /dev/null 2>&1; then
        rm -f "$PIDFILE"
    fi
fi

# 单实例保护
EXISTING_PIDS=$(pgrep -f "$PROC_REGEX" || true)
if [ -n "$EXISTING_PIDS" ]; then
    echo "⚠️  检测到 Trade_db 已在运行，PID: $EXISTING_PIDS"
    echo "请先执行 ./stop.sh，再执行 ./run.sh"
    exit 1
fi

export PYTHONPATH="$(pwd)"
echo "🚀 正在后台启动 Trade_db (nohup)..."
nohup python3 "$(pwd)/main.py" > logs/system_run.log 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"

# 健康检查（等待 10 秒确认进程未秒退）
for i in {1..10}; do
    sleep 1
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "❌ Trade_db 启动失败（进程秒退）"
        echo "请检查 logs/system_run.log / trade_db.log"
        rm -f "$PIDFILE"
        exit 1
    fi
done

echo "✅ Trade_db 启动成功！"
echo "-----------------------------------"
echo "进程 ID (PID)  : $PID"
echo "业务日志记录   : trade_db.log"
echo "-----------------------------------"
echo "💡 查看实时日志: tail -f trade_db.log"
