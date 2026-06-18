import asyncio
import json
import time

import websockets

from src.utils.log import get_logger

logger = get_logger(__name__)


class OKXTickStreamer:
    def __init__(self, symbol="ETH-USDT-SWAP", multiplier=0.1, on_tick_callback=None):
        self.symbol = symbol
        self.on_tick_callback = on_tick_callback  # 核心：通过回调把数据抛给策略大脑
        # 使用 AWS 专线域名，在东京节点极其稳定
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.multiplier = multiplier  # OKX永续合约的sz指的是张数，1张ETH合约等于0.1个ETH

    async def connect(self):
        """建立 WebSocket 连接，保持心跳与断线重连"""
        subscribe_msg = {
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": self.symbol}]
        }

        while True:
            try:
                logger.info(f"🚀 [数据层] 正在连接 OKX 订单流极速通道 ({self.symbol})...")
                async with websockets.connect(self.ws_url) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("✅ [数据层] 接入成功！持续监听并清洗 Ticks 数据...")

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)

                        # 如果包含交易数据，且上层注册了回调函数
                        if 'data' in data and self.on_tick_callback:
                            for trade in data['data']:
                                # 提纯数据：只保留我们算法需要的 4 个核心字段
                                tick_clean = {
                                    'price': float(trade['px']),
                                    'size': float(trade['sz']) * self.multiplier,
                                    'side': trade['side'],
                                    'ts': float(trade['ts']) / 1000.0,  # 转为秒级时间戳
                                    'recv_ts': time.time()
                                }
                                # 将干净的字典抛给 Engine3Commander 的 on_tick 函数
                                await self.on_tick_callback(tick_clean)

            except Exception as e:
                logger.error(f"❌ [数据层] 链路断开，准备 3 秒后重连: {e}")
                await asyncio.sleep(3)
