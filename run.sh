#!/bin/bash
# 启动 Trade_db 服务并在后台运行
# Usage: ./run.sh

# 进入项目根目录
cd "$(dirname "$0")"

# 确保日志目录存在
mkdir -p logs

# 检查是否已经运行
if [ -f "logs/trade_db.pid" ]; then
    PID=$(cat logs/trade_db.pid)
    if ps -p $PID > /dev/null; then
        echo "⚠️ Trade_db 服务已经在运行 (PID: $PID)。"
        echo "如需重启，请先执行 ./stop.sh"
        exit 1
    else
        echo "清理已失效的 PID 文件。"
        rm logs/trade_db.pid
    fi
fi

echo "🚀 正在后台启动 Trade_db..."

# 设置环境变量并使用 nohup 运行主程序
export PYTHONPATH="$(pwd)"
nohup python3 main.py > logs/system_run.log 2>&1 &

# 获取并保存后台进程的 PID
PID=$!
echo $PID > logs/trade_db.pid

echo "✅ Trade_db 启动成功！"
echo "-----------------------------------"
echo "进程 ID (PID)  : $PID"
echo "系统标准输出   : logs/system_run.log"
echo "业务日志记录   : trade_db.log"
echo "-----------------------------------"
echo "💡 提示: 运行以下命令可以查看实时日志："
echo "   tail -f trade_db.log"
