#!/bin/bash
# 启动 Trade_db 服务并在后台运行
# Usage: ./run.sh

# 进入项目根目录
cd "$(dirname "$0")"

# 如果已安装 systemd 服务，默认禁止手工 run.sh，避免重复实例
if systemctl list-unit-files 2>/dev/null | grep -q ^trade_db.service; then
    if [ "${MANUAL_RUN:-0}" != "1" ]; then
        echo "⚠️ 检测到 trade_db.service 已安装。"
        echo "为避免重复进程，请使用: systemctl start trade_db.service"
        echo "如需强制手工启动，请执行: MANUAL_RUN=1 ./run.sh"
        exit 1
    fi
fi

# 确保日志目录存在
mkdir -p logs

PIDFILE="logs/trade_db.pid"
# 使用更精确的匹配：包含 python 且同时包含 Trade_db 和 main.py
PROC_REGEX="[p]ython3?.*Trade_db/main\.py"

# 清理失效 pidfile
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "清理已失效的 PID 文件。"
        rm -f "$PIDFILE"
    fi
fi

# 单实例保护：如果已有 main.py 进程，直接拒绝重复拉起
EXISTING_PIDS=$(pgrep -f "$PROC_REGEX" || true)
if [ -n "$EXISTING_PIDS" ]; then
    echo "⚠️ 检测到 Trade_db 已在运行，PID: $EXISTING_PIDS"
    echo "请先执行 ./stop.sh，再执行 ./run.sh"
    exit 1
fi

echo "🚀 正在后台启动 Trade_db..."

# 设置环境变量并使用 nohup 运行主程序
export PYTHONPATH="$(pwd)"

# 确保使用的是绝对路径来启动，以便进程名中包含完整的 Trade_db/main.py，便于后续精确 kill
nohup python3 "$(pwd)/main.py" > logs/system_run.log 2>&1 &

# 获取并保存后台进程的 PID
PID=$!
echo "$PID" > "$PIDFILE"

# 健康检查：等待最多 10 秒确认进程未秒退
for _ in {1..10}; do
    if ps -p "$PID" > /dev/null 2>&1; then
        sleep 1
    else
        echo "❌ Trade_db 启动失败：进程秒退。"
        echo "请检查 logs/system_run.log / trade_db.log"
        exit 1
    fi
done

echo "✅ Trade_db 启动成功！"
echo "-----------------------------------"
echo "进程 ID (PID)  : $PID"
echo "系统标准输出   : logs/system_run.log"
echo "业务日志记录   : trade_db.log"
echo "-----------------------------------"
echo "💡 提示: 运行以下命令可以查看实时日志："
echo "   tail -f trade_db.log"
