#!/bin/bash
# 重启 Trade_db 服务（先 stop 再 start）
# Usage: ./restart.sh

cd "$(dirname "$0")"

# ── systemd 路径 ──────────────────────────────────────────────
if systemctl list-unit-files 2>/dev/null | grep -q ^trade_db.service; then
    echo "🔄 正在重启 trade_db.service (systemd)..."
    systemctl restart trade_db.service
    sleep 2
    if systemctl is-active --quiet trade_db.service; then
        echo "✅ Trade_db 重启成功（systemd）"
        echo "💡 查看日志: journalctl -u trade_db.service -f"
        echo "   或:       tail -f trade_db.log"
    else
        echo "❌ 重启失败，查看日志:"
        echo "   journalctl -u trade_db.service -n 30 --no-pager"
        exit 1
    fi
    exit 0
fi

# ── 手工 nohup 路径 ──────────────────────────────────────────
echo "🔄 正在重启 Trade_db (stop → run)..."
bash "$(dirname "$0")/stop.sh"
sleep 1
bash "$(dirname "$0")/run.sh"
