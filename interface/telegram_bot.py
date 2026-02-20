
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

class TelegramBot:
    """
    Telegram Bot 服务
    """
    def __init__(self, token: str, allowed_users: List[int] = None):
        self.token = token
        self.allowed_users = allowed_users or []
        self.app = ApplicationBuilder().token(token).build()
        # self.llm = get_llm_client() # Replaced by Agent
        
        self._register_handlers()

    def run(self):
        """启动 Bot (Blocking)"""
        logger.info("Starting Telegram Bot...")
        self.app.run_polling()

    async def _check_auth(self, update: Update) -> bool:
        """鉴权"""
        user_id = update.effective_user.id
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
        self.app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self.handle_message))

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
            "• /quote 600519 (查询茅台)\n"
            "• /quote 00700 (查询腾讯)\n"
            "• /chain AI算力 (分析产业链)\n"
            "• 直接发送问题: '最近光模块有什么利好？'"
        )
        await update.message.reply_text(text, parse_mode="HTML")

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
            
            # Use HTML Renderer
            html_response = TelegramHTMLRenderer.render(response)
            
            if not html_response or not html_response.strip():
                # Fallback for empty content
                html_response = response if response else "❌ 分析完成，但生成的内容为空。"
            
            if len(html_response) > 4000:
                # Simple split
                await update.message.reply_text(html_response[:4000], parse_mode="HTML")
                await update.message.reply_text(html_response[4000:], parse_mode="HTML")
            else:
                # Use reply_text instead of edit_text to ensure notification
                await update.message.reply_text(html_response, parse_mode="HTML") 
                
        except Exception as e:
            await update.message.reply_text(f"❌ 分析失败: {e}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理自然语言消息 (Agent 版)"""
        if not await self._check_auth(update): return
        
        user_text = update.message.text
        
        # Indicate typing
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        
        try:
            # Run in thread
            response = await asyncio.to_thread(agent_executor.run, user_text)
            
            # Use HTML Renderer
            html_response = TelegramHTMLRenderer.render(response)
            
            # Simple split for long messages
            if len(html_response) > 4000:
                await update.message.reply_text(html_response[:4000], parse_mode="HTML")
                await update.message.reply_text(html_response[4000:], parse_mode="HTML")
            else:
                await update.message.reply_text(html_response, parse_mode="HTML")
                
        except Exception as e:
            await update.message.reply_text(f"Agent Error: {e}")

# Factory
def create_bot():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.warning("TELEGRAM_BOT_TOKEN not set.")
        return None
        
    # Optional: ALLOWED_USER_IDS="12345,67890"
    allowed_ids_str = os.getenv("ALLOWED_USER_IDS", "")
    allowed_ids = [int(i) for i in allowed_ids_str.split(",") if i.strip()]
    
    return TelegramBot(token, allowed_ids)
