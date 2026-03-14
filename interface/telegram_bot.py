
"""
Telegram Bot Interface
"""
import re
import logging
import os
import asyncio
from typing import List
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from core.agent import agent_executor

logger = logging.getLogger(__name__)

class TelegramHTMLRenderer:
    """
    Robust Markdown to Telegram HTML Converter
    State-machine based approach to handle LLM's messy output safely.
    """
    @staticmethod
    def render(text: str) -> str:
        if not text: return ""
        
        # 1. Escape HTML special characters first
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        
        # 2. Process Line by Line
        lines = text.split('\n')
        html_lines = []
        in_code_block = False
        
        for line in lines:
            # Code Block Toggle
            if line.strip().startswith('```'):
                in_code_block = not in_code_block
                if in_code_block:
                    html_lines.append('<pre>')
                else:
                    html_lines.append('</pre>')
                continue
                
            if in_code_block:
                html_lines.append(line)
                continue
                
            # --- Normal Text Processing ---
            
            # Headers (### Title -> <b>Title</b>)
            # Telegram doesn't support headers, use Bold + Uppercase or just Bold
            line = re.sub(r'^(#{1,6})\s+(.*)', r'<b>\2</b>', line)
            
            # Bold (**text**)
            line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', line)
            
            # Italic (*text*) - careful with bullet points
            # Only match * if not at start of line (list)
            # This regex is simplified and might miss some cases, but safer
            # line = re.sub(r'(?<!^)\*(.*?)\*', r'<i>\1</i>', line) # Too risky
            
            # Inline Code (`text`)
            line = re.sub(r'`([^`]*)`', r'<code>\1</code>', line)
            
            # Lists (- item or * item) -> • item
            if re.match(r'^\s*[-*]\s+', line):
                line = re.sub(r'^\s*[-*]\s+', '• ', line)
                
            # Numbered Lists (1. item) -> Keep as is
            
            html_lines.append(line)
            
        # 3. Join with newlines
        return '\n'.join(html_lines)

# Global variable to store the last chat ID for notifications
LAST_CHAT_ID = None
bot_instance = None

class TelegramBot:
    """
    Telegram Bot 服务
    """
    def __init__(self, token: str, allowed_users: List[int] = None, allowed_groups: List[int] = None):
        global bot_instance
        self.token = token
        self.allowed_users = allowed_users or []
        self.allowed_groups = allowed_groups or []  # 允许群组ID，群内所有成员均可使用
        self.app = ApplicationBuilder().token(token).build()
        
        self._register_handlers()
        bot_instance = self

    @staticmethod
    def _strip_thought(text: str) -> str:
        """过滤 ReAct Agent 内部的 Thought/Observation 推理链，只保留干净的输出"""
        clean_lines = []
        skip_prefixes = ("Thought:", "Observation:", "Question:", "SEARCH:", "QUOTE:", "DB:")
        for line in text.split('\n'):
            stripped = line.strip()
            if any(stripped.startswith(p) for p in skip_prefixes):
                continue
            clean_lines.append(line)
        return '\n'.join(clean_lines).strip()

    @staticmethod
    async def send_alert(message: str):
        """主动发送报警消息"""
        if not bot_instance or not bot_instance.app:
            logger.warning("Bot not initialized, cannot send alert.")
            return
            
        if not LAST_CHAT_ID:
            logger.warning("No chat ID found (User hasn't interacted yet). Cannot send alert.")
            return
            
        try:
            await bot_instance.app.bot.send_message(
                chat_id=LAST_CHAT_ID, 
                text=message, 
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    def run(self):
        """启动 Bot (Blocking)"""
        logger.info("Starting Telegram Bot...")
        self.app.run_polling()

    async def _check_auth(self, update: Update) -> bool:
        """鉴权：允许名单用户 + 允许群组的所有成员"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Auto-record the chat ID for alerts
        global LAST_CHAT_ID
        LAST_CHAT_ID = chat_id

        # 群组白名单：群内所有成员均可使用，无需单独授权
        if self.allowed_groups and chat_id in self.allowed_groups:
            return True

        # 私聊：仍走个人 ID 白名单
        if self.allowed_users and user_id not in self.allowed_users:
            await update.message.reply_text(f"⛔️ Access Denied (ID: {user_id})")
            return False
        return True

    def _register_handlers(self):
        """注册指令"""
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("quote", self.cmd_quote))
        self.app.add_handler(CommandHandler("chain", self.cmd_chain))
        self.app.add_handler(CommandHandler("monitor", self.cmd_monitor))
        # 群组直接可用的快捷指令（斠山指令，不依赖隱私模式）
        self.app.add_handler(CommandHandler("add", self.cmd_add))
        self.app.add_handler(CommandHandler("del", self.cmd_del))
        self.app.add_handler(CommandHandler("list", self.cmd_list))
        self.app.add_handler(CommandHandler("trend", self.cmd_trend))
        self.app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self.handle_message))

    async def cmd_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """股票监控管理"""
        if not await self._check_auth(update): return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "📉 **智能异动监控**\n\n"
                "指令示例:\n"
                "/monitor add 腾讯  (添加监控)\n"
                "/monitor list     (查看列表)\n"
                "/monitor del 00700 (移除监控)",
                parse_mode="Markdown"
            )
            return

        action = args[0].lower()
        
        try:
            from modules.monitor.manager import MonitorManager
            
            if action == "add":
                if len(args) < 2:
                    await update.message.reply_text("请提供股票名称或代码。")
                    return
                query = " ".join(args[1:])
                await update.message.reply_text(f"🔍 正在识别股票: {query}...")
                
                # Run in thread to avoid blocking
                msg = await asyncio.to_thread(MonitorManager.add_stock, query)
                await update.message.reply_text(msg)
                
            elif action == "list":
                msg = await asyncio.to_thread(MonitorManager.list_stocks)
                await update.message.reply_text(msg, parse_mode="Markdown")
                
            elif action == "del" or action == "remove":
                if len(args) < 2:
                    await update.message.reply_text("请提供要移除的代码。")
                    return
                symbol = args[1]
                msg = await asyncio.to_thread(MonitorManager.remove_stock, symbol)
                await update.message.reply_text(msg)
                
            else:
                await update.message.reply_text("未知指令。请使用 add, list, 或 del。")
                
        except Exception as e:
            await update.message.reply_text(f"❌ 操作失败: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_auth(update): return
        text = (
            "🚀 <b>Trade_db Agent Online</b>\n\n"
            "我是您的 AI 投研助理。请发送指令或直接提问。\n\n"
            "/quote &lt;代码&gt; - 查询实时行情\n"
            "/chain &lt;产业&gt; - 挖掘产业链\n"
            "/help - 查看帮助"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_auth(update): return
        text = (
            "<b>可用指令</b>:\n"
            "• /add 腾讯  (添加自选股监控)\n"
            "• /del 00700  (移除自选股监控)\n"
            "• /list  (查看当前监控列表)\n"
            "• /quote 600519 (查询茅台行情)\n"
            "📉 /trend [N] - 输出最近 N天(默认7天) 多市场趋势简报\n"
            "   (支持: A股/港股/美股/期货，N建议7或30)\n"
            "🔍 /chain <标的> [指令] - 组合查询 (例如: /chain 腾讯 最新消息)\n"
            "• 直接发送问题: '最近光模块有什么利好？'"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    # ──────────────────────────────────────────────────────
    # 群组快捷指令：/add  /del  /list
    # ──────────────────────────────────────────────────────

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/add 股票名称 - 添加监控（群组可用）"""
        if not await self._check_auth(update): return
        if not context.args:
            await update.message.reply_text("用法: /add 腾讯  或  /add 00700")
            return
        query = " ".join(context.args)
        await update.message.reply_text(f"🔍 正在识别: {query}...")
        from modules.monitor.manager import MonitorManager
        chat_id = update.effective_chat.id
        msg = await asyncio.to_thread(MonitorManager.add_stock, query, chat_id)
        await update.message.reply_text(msg)

    async def cmd_del(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/del 代码 - 移除监控（群组可用）"""
        if not await self._check_auth(update): return
        if not context.args:
            await update.message.reply_text("用法: /del 00700")
            return
        symbol = context.args[0]
        from modules.monitor.manager import MonitorManager
        msg = await asyncio.to_thread(MonitorManager.remove_stock, symbol)
        await update.message.reply_text(msg)

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/list - 查看监控列表（群组可用）"""
        if not await self._check_auth(update): return
        from modules.monitor.manager import MonitorManager
        msg = await asyncio.to_thread(MonitorManager.list_stocks)
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def cmd_trend(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/trend [days] - 计算并分析市场 N天 趋势榜单"""
        if not await self._check_auth(update): return
        
        days = 7
        if context.args and context.args[0].isdigit():
            days = int(context.args[0])
            
        progress_msg = await update.message.reply_text(f"⏳ 正在生成 {days} 日趋势简报（含现价与涨幅），请稍候...")

        from modules.monitor.trend_report_service import TrendReportService
        import logging
        logger = logging.getLogger(__name__)

        try:
            report_text = await asyncio.to_thread(TrendReportService.build_report, days)
            await progress_msg.delete()

            # Telegram 单条消息最多 4096 字符，安全限制在 4000 左右进行拆包发送
            chunk_size = 4000
            for i in range(0, len(report_text), chunk_size):
                await update.message.reply_text(report_text[i:i+chunk_size])

        except Exception as e:
            logger.error(f"Trend cmd failed: {e}", exc_info=True)
            await progress_msg.edit_text(f"❌ 趋势简报生成失败: {e}")

    async def cmd_quote(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """查询行情 (Agent 版)"""
        if not await self._check_auth(update): return
        
        args = context.args
        if not args:
            await update.message.reply_text("请提供代码，例如: /quote 00700")
            return

        symbol = args[0]
        # 让 Agent 去处理，它会调用 get_quote 工具
        response = agent_executor.run(f"查询股票 {symbol} 的实时行情")
        
        # HTML
        text = TelegramHTMLRenderer.render(response)
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_chain(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """产业链分析 (Agent 版)"""
        if not await self._check_auth(update): return
        
        industry = " ".join(context.args)
        if not industry:
            await update.message.reply_text("请提供产业名称，例如: /chain 光模块")
            return
            
        # Send initial message (Keep it simple)
        await update.message.reply_text(f"🔍 正在深度挖掘【{industry}】，这可能需要 30-60 秒，请耐心等待...")
        
        try:
            # Run Agent logic
            response = await asyncio.to_thread(
                agent_executor.run, 
                f"深度分析 {industry} 产业链，包括上中下游和核心龙头股。如果本地不知道，请联网搜索。"
            )
            
            # 过滤掉 Thought/Observation 推理链，只保留最终输出
            clean_response = TelegramBot._strip_thought(response)
            html_response = TelegramHTMLRenderer.render(clean_response)
            
            if not html_response or not html_response.strip():
                html_response = clean_response if clean_response else "❌ 分析完成，但生成的内容为空。"
            
            if len(html_response) > 4000:
                await update.message.reply_text(html_response[:4000], parse_mode="HTML")
                await update.message.reply_text(html_response[4000:], parse_mode="HTML")
            else:
                await update.message.reply_text(html_response, parse_mode="HTML") 
                
        except Exception as e:
            await update.message.reply_text(f"❌ 分析失败: {e}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理文本消息"""
        if not await self._check_auth(update): return
        
        text = update.message.text.strip()
        
        # 快捷指令处理
        if text.startswith(("+", "add ", "监控 ")):
            # 添加监控: +腾讯, add 腾讯, 监控 腾讯
            query = text.lstrip("+").replace("add ", "").replace("监控 ", "").strip()
            if query:
                from modules.monitor.manager import MonitorManager
                await update.message.reply_text(f"🔍 正在识别: {query}...")
                
                # Pass chat_id
                chat_id = update.effective_chat.id
                msg = await asyncio.to_thread(MonitorManager.add_stock, query, chat_id)
                await update.message.reply_text(msg)
            return
            
        if text.startswith(("-", "del ", "rm ", "删除 ")):
            # 删除监控: -00700, del 00700
            symbol = text.lstrip("-").replace("del ", "").replace("rm ", "").replace("删除 ", "").strip()
            if symbol:
                from modules.monitor.manager import MonitorManager
                msg = await asyncio.to_thread(MonitorManager.remove_stock, symbol)
                await update.message.reply_text(msg)
            return
            
        if text.lower() in ("list", "ls", "监控列表", "自选股"):
            # 查看列表
            from modules.monitor.manager import MonitorManager
            msg = await asyncio.to_thread(MonitorManager.list_stocks)
            await update.message.reply_text(msg, parse_mode="Markdown")
            return

        # 默认行为：调用 Agent 进行对话
        status_msg = await update.message.reply_text("🤖 思考中...")
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        
        try:
            # Run Agent logic
            response = await asyncio.to_thread(agent_executor.run, text)
            
            # 过滤掉 Thought/Observation 推理链
            clean_response = TelegramBot._strip_thought(response)
            html_response = TelegramHTMLRenderer.render(clean_response)
            
            if not html_response or not html_response.strip():
                html_response = clean_response if clean_response else "❌ 生成内容为空。"
            
            if len(html_response) > 4000:
                await status_msg.edit_text(html_response[:4000], parse_mode="HTML")
                await update.message.reply_text(html_response[4000:], parse_mode="HTML")
            else:
                await status_msg.edit_text(html_response, parse_mode="HTML")
                
        except Exception as e:
            await status_msg.edit_text(f"❌ 错误: {e}")

# Factory
def create_bot():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.warning("TELEGRAM_BOT_TOKEN not set.")
        return None
        
    # Optional: ALLOWED_USER_IDS="12345,67890"
    allowed_ids_str = os.getenv("ALLOWED_USER_IDS", "")
    allowed_ids = [int(i) for i in allowed_ids_str.split(",") if i.strip()]
    
    # Optional: ALLOWED_GROUP_IDS="-100123456789" (群组ID通常为负数)
    allowed_groups_str = os.getenv("ALLOWED_GROUP_IDS", "")
    allowed_groups = [int(i) for i in allowed_groups_str.split(",") if i.strip()]
    
    return TelegramBot(token, allowed_ids, allowed_groups)
