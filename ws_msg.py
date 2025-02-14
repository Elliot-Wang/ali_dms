import asyncio
import json
import ssl
import time
from enum import Enum
from threading import Thread

import pandas as pd
import websockets

TYPE_LIST = ['execResList', 'execResQueryId', 'execResult']


class WsStatus(Enum):
    INIT = "init"
    WAITING = "waiting"
    FETCHED = "fetched"
    CONNECTED = "connected"
    ERROR = "error"


class LongWS:
    def __init__(self, cookie: str):
        self.status: WsStatus = WsStatus.INIT
        self.data: dict = None
        self.err_msg = None
        self.cookie: str = cookie
        self.sql: str = None
        self.db: int = None
        self.ws = None

    def sql_query(self, sql, db=None):
        if db is None:
            db = self.db
        if sql is None:
            return
        asyncio.run(self._exec_sql(sql, db))
        for i in range(100):
            if self.status == WsStatus.FETCHED:
                self.status = WsStatus.CONNECTED
                return self.data
            time.sleep(0.1)
        return None

    async def _exec_sql(self, sql, db):
        if self.status == WsStatus.WAITING:
            print("execute waiting...")
            return
        if self.status == WsStatus.FETCHED:
            print("data not fetched...")
            return
        if sql is None or db is None:
            return
        # 在这里可以发送一个初始消息或执行其他初始化操作
        req = {"type": "execute", "data": {
            "sql": sql, "dbId": db,
            "logic": False, "characterType": "", "switchedHostPort": "", "ignoreConfirm": False,
            "blobDirectDisplay": False, "blobToHex": False, "binaryToHex": False, "sessionPersistence": False,
            "dbType": "mysql", "excutionAbort": False}, "token": ""}
        self.status = WsStatus.WAITING
        # Send a message to the server
        await self.ws.send(json.dumps(req))

    async def _on_open(self):
        print("Connection opened")
        if self.sql is None or self.db is None:
            return
        await self._exec_sql(self.sql, self.db)

    async def _on_close(self, reason):
        print("Connection closed", reason)

    async def _connect(self):
        # mac: depend on Python version
        # open /Applications/Python\ 3.6/Install\ Certificates.command
        # to enable ssl
        uri = "wss://dms.aliyun.com/ws/newwebsql/query"  # Secure WebSocket server address
        headers = {
            "Cookie": self.cookie,
            "Connection": "Upgrade",
            "Upgrade": "websocket",
            "Origin": "https://dms.aliyun.com",
            "Pragma": "no-cache",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Sec-WebSocket-Version": "13",
            "Sec-WebSocket-Extensions": "permessage-deflate;client_max_window_bits=15",
        }
        # SSL context for secure connection
        ssl_context = ssl.create_default_context()

        try:
            # Establish the WebSocket connection with custom headers
            async with websockets.connect(uri, ssl=ssl_context, additional_headers=headers) as websocket:
                self.ws = websocket
                # 当连接成功建立后调用on_open
                await self._on_open()
                self.status = WsStatus.CONNECTED

                # 在这里可以添加接收消息的循环或其他业务逻辑
                async for message in websocket:
                    msg = json.loads(message)
                    m_type = msg['type']
                    print(f"Received: {message}")
                    # 根据收到的消息做出响应...
                    if m_type == TYPE_LIST[2]:
                        self.data = msg
                        self.status = WsStatus.FETCHED
        except websockets.ConnectionClosed as err:
            self.status = WsStatus.ERROR
            self.err_msg = err.rcvd.reason
            # 连接意外关闭时会进入这里
            await self._on_close(err.rcvd.reason)

    def connect(self):
        if self.status != WsStatus.INIT:
            return
        loop = asyncio.new_event_loop()
        t = Thread(target=start_background_loop, args=(loop,), daemon=True)
        t.start()

        asyncio.run_coroutine_threadsafe(self._connect(), loop)
        for i in range(50):
            if self.status != WsStatus.INIT:
                return
            time.sleep(0.1)


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()
