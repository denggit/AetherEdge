import asyncio
import json
import time

import websockets

from src.utils.log import get_logger

logger = get_logger(__name__)


class OKXBooksStreamer:
    def __init__(self, symbol="ETH-USDT-SWAP", multiplier=0.1, on_book_callback=None):
        self.symbol = symbol
        self.multiplier = multiplier
        # 核心：通过回调把订单簿增量数据抛给 MarketContext
        self.on_book_callback = on_book_callback
        # 保持和你 okx_stream 一致的域名
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"

    async def connect(self):
        subscribe_msg = {
            "op": "subscribe",
            "args": [{"channel": "books", "instId": self.symbol}]
        }

        while True:
            try:
                logger.info(f"📚 [数据层] 正在连接 OKX 订单簿专属通道 ({self.symbol})...")
                async with websockets.connect(self.ws_url) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("✅ [数据层] 订单簿接入成功！持续监听 400 档盘口深度...")

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)

                        if 'data' in data and self.on_book_callback:
                            book_data = data['data'][0]

                            # 👇 核心修复：在这里把挂单的张数，清洗成真实的 ETH 数量！
                            cleaned_bids = [[float(item[0]), float(item[1]) * self.multiplier] for item in
                                            book_data.get('bids', [])]
                            cleaned_asks = [[float(item[0]), float(item[1]) * self.multiplier] for item in
                                            book_data.get('asks', [])]

                            cleaned_book_data = {
                                'bids': cleaned_bids,
                                'asks': cleaned_asks,
                                'ts': float(book_data['ts']) / 1000 if 'ts' in book_data else None,
                                'recv_ts': time.time()
                            }

                            self.on_book_callback(cleaned_book_data)

            except Exception as e:
                logger.error(f"❌ [数据层] 订单簿链路断开，准备 3 秒后重连: {e}")
                await asyncio.sleep(3)
