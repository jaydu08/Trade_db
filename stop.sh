#!/bin/bash
# 停止 Trade_db 后台服务
# Usage: ./stop.sh

cd "$(dirname "$0")"

if [ -f "logs/trade_db.pid" ]; then
    PID=$(cat logs/trade_db.pid)
    
    # 检查进程是否真实存在
    if ps -p $PID > /dev/null; then
        echo "🛑 正在停止 Trade_db 服务 (PID: $PID)..."
        kill $PID
        
        # 宽限期等待优雅退出
        sleep 2
        if ps -p $PID > /dev/null; then
            echo "⚠️ 服务未能正常退出，尝试强制终止 (kill -9)..."
            kill -9 $PID
        fi
        
        echo "✅ Trade_db 已经停止运行。"
    else
        echo "Trade_db 服务未运行 (PID 映射进程不存在)。"
    fi
    
    # 清理 PID 文件
    rm logs/trade_db.pid
else
    echo "未找到运行记录 (logs/trade_db.pid)。服务似乎并未以常规途径启动。"
    
    # 后备查找逻辑
    PIDS=$(pgrep -f "python3 main.py")
    if [ ! -z "$PIDS" ]; then
        echo "🔍 发现游离的 Trade_db 进程，正在强制清理..."
        kill -9 $PIDS
        echo "清理完成。"
    fi
fi
