import requests
import json
import time
import threading
import logging
from urllib.parse import urljoin
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class CaixinClient:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(CaixinClient, cls).__new__(cls)
                    cls._instance._init()
        return cls._instance

    def _init(self):
        self.base_url = "https://appai.caixin.com"
        self.token = "BB1Q3wv6seGRZvUA2Cu11g==" # Should be in env
        self.sse_url = f"{self.base_url}/mcpsse/sse?token={self.token}"
        self.post_url = None
        self.responses = {} 
        self.req_lock = threading.Lock()
        self.running = False
        self.thread = None
        self.tool_name = "search_caixin_content" # Default known tool

    def start(self):
        with self._lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(target=self._sse_reader)
            self.thread.daemon = True
            self.thread.start()
            
            # Wait for endpoint
            start_time = time.time()
            while not self.post_url and time.time() - start_time < 10:
                time.sleep(0.1)
            
            if self.post_url:
                # Initialize MCP
                self._send_jsonrpc("initialize", {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "trade-db-client", "version": "1.0.0"}
                }, req_id=1)
                self._send_jsonrpc("notifications/initialized", {}, req_id=None)

    def _sse_reader(self):
        headers = {"Accept": "text/event-stream"}
        while self.running:
            try:
                logger.info(f"Connecting to Caixin SSE: {self.sse_url}")
                with requests.get(self.sse_url, stream=True, headers=headers, timeout=60) as response:
                    response.raise_for_status()
                    logger.info("Caixin SSE Connected.")
                    
                    for line in response.iter_lines(decode_unicode=True):
                        if not self.running: break
                        if not line: continue
                        
                        if line.startswith("data: "):
                            data = line[6:].strip()
                            
                            if "messages" in data and not self.post_url:
                                url = data
                                if not url.startswith("http"):
                                    url = urljoin(self.base_url, url)
                                self.post_url = url
                                logger.info(f"Caixin Endpoint: {self.post_url}")
                                continue
                            
                            try:
                                msg = json.loads(data)
                                if "id" in msg:
                                    with self.req_lock:
                                        self.responses[msg["id"]] = msg
                            except:
                                pass
            except Exception as e:
                logger.error(f"Caixin SSE Error: {e}")
                time.sleep(5) # Retry delay

    def _send_jsonrpc(self, method, params, req_id):
        if not self.post_url:
            return None
        
        payload = {"jsonrpc": "2.0", "method": method, "id": req_id, "params": params or {}}
        try:
            requests.post(self.post_url, json=payload, timeout=10)
            
            if req_id is None:
                return None
                
            # Wait for response
            start_time = time.time()
            while time.time() - start_time < 30: # 30s timeout for tool calls
                with self.req_lock:
                    if req_id in self.responses:
                        return self.responses.pop(req_id)
                time.sleep(0.1)
            logger.warning(f"Timeout waiting for response ID {req_id}")
        except Exception as e:
            logger.error(f"Caixin Request Failed: {e}")
        return None

    def search(self, query: str, start_date: str = None, end_date: str = None) -> str:
        """
        Synchronous search method
        """
        if not self.running:
            self.start()
            
        if not self.post_url:
            return "Error: Caixin service not connected."

        import random
        req_id = random.randint(1000, 99999)
        
        args = {"keyword": query}
        if start_date: args["startTime"] = start_date
        if end_date: args["endTime"] = end_date
        
        logger.info(f"Calling Caixin Search: {args}")
        
        res = self._send_jsonrpc("tools/call", {
            "name": self.tool_name,
            "arguments": args
        }, req_id=req_id)
        
        if res and "result" in res:
            content = res["result"].get("content", [])
            text_res = ""
            for item in content:
                if item.get("type") == "text":
                    text_res += item.get("text", "") + "\n"
            return text_res
        
        return "No results or error."

caixin_client = CaixinClient()
