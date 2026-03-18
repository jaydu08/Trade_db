#!/bin/bash
# 停止 Trade_db 后台服务
# Usage: ./stop.sh

cd "$(dirname "$0")"
PIDFILE="logs/trade_db.pid"
PROC_REGEX="[p]ython3?.*Trade_db/main\.py"

if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    
    # 检查进程是否真实存在
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "🛑 正在停止 Trade_db 服务 (PID: $PID)..."
        kill "$PID"
        
        # 宽限期等待优雅退出
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "⚠️ 服务未能正常退出，尝试强制终止 (kill -9)..."
            kill -9 "$PID"
        fi
        
        echo "✅ Trade_db 已经停止运行。"
    else
        echo "Trade_db 服务未运行 (PID 映射进程不存在)。"
    fi
    
    # 清理 PID 文件
    rm -f "$PIDFILE"
else
    echo "未找到运行记录 (logs/trade_db.pid)。服务似乎并未以常规途径启动。"
fi

# 兜底：清理所有游离 main.py 进程，避免旧进程继续抢 Telegram 更新
PIDS=$(pgrep -f "$PROC_REGEX" || true)
if [ -n "$PIDS" ]; then
    echo "🔍 发现游离的 Trade_db 进程，正在清理: $PIDS"
    kill $PIDS 2>/dev/null || true
    sleep 1
    PIDS_REMAIN=$(pgrep -f "$PROC_REGEX" || true)
    if [ -n "$PIDS_REMAIN" ]; then
        echo "⚠️ 仍有残留进程，执行强制清理: $PIDS_REMAIN"
        kill -9 $PIDS_REMAIN 2>/dev/null || true
    fi
    echo "清理完成。"
fi
