#!/bin/bash
# 停止 Trade_db 服务
# Usage: ./stop.sh
#
# 优先使用 systemd 管理（已安装 trade_db.service 时自动代理）；
# 未安装 systemd 服务时退回 pid/pgrep 手工停止。

cd "$(dirname "$0")"
PIDFILE="logs/trade_db.pid"
PROC_REGEX="[p]ython3?.*Trade_db/main\.py"

# ── systemd 路径 ──────────────────────────────────────────────
if systemctl list-unit-files 2>/dev/null | grep -q ^trade_db.service; then
    STATUS=$(systemctl is-active trade_db.service 2>/dev/null)
    if [ "$STATUS" != "active" ]; then
        echo "ℹ️  trade_db.service 当前未在运行（状态: ${STATUS:-inactive}）。"
    else
        echo "🛑 正在停止 trade_db.service (systemd)..."
        systemctl stop trade_db.service
        # 等待最多 8 秒确认停止
        for i in {1..8}; do
            sleep 1
            if ! systemctl is-active --quiet trade_db.service; then
                echo "✅ Trade_db 已停止。"
                break
            fi
        done
        if systemctl is-active --quiet trade_db.service; then
            echo "⚠️  停止超时，请手动检查: systemctl status trade_db.service"
        fi
    fi

    # 兜底：清理 systemd 之外的游离进程（异常情况下可能存在）
    ORPHANS=$(pgrep -f "$PROC_REGEX" || true)
    if [ -n "$ORPHANS" ]; then
        echo "🔍 发现游离的 Trade_db 进程，正在清理: $ORPHANS"
        kill $ORPHANS 2>/dev/null || true
        sleep 1
        REMAIN=$(pgrep -f "$PROC_REGEX" || true)
        [ -n "$REMAIN" ] && kill -9 $REMAIN 2>/dev/null || true
        echo "游离进程清理完成。"
    fi
    exit 0
fi

# ── 手工 nohup 路径（未安装 systemd 服务时使用）──────────────
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "🛑 正在停止 Trade_db (PID: $PID)..."
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "⚠️  进程未响应，执行强制终止..."
            kill -9 "$PID"
        fi
        echo "✅ Trade_db 已停止。"
    else
        echo "ℹ️  PID $PID 对应的进程不存在，可能已停止。"
    fi
    rm -f "$PIDFILE"
else
    echo "ℹ️  未找到 PID 文件，服务可能未在运行。"
fi

# 兜底：清理所有游离 main.py 进程
PIDS=$(pgrep -f "$PROC_REGEX" || true)
if [ -n "$PIDS" ]; then
    echo "🔍 发现游离的 Trade_db 进程，正在清理: $PIDS"
    kill $PIDS 2>/dev/null || true
    sleep 1
    REMAIN=$(pgrep -f "$PROC_REGEX" || true)
    [ -n "$REMAIN" ] && kill -9 $REMAIN 2>/dev/null || true
    echo "清理完成。"
fi
