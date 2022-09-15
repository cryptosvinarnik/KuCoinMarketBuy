import base64
import hashlib
import hmac
import json
import time

import aiohttp
from loguru import logger


class _KuCoinContextManager():
    def __init__(self, api_key: str, passphrase: str, signature: str, timestamp: str):
        self.headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-KEY": api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }

    async def __aenter__(self) -> aiohttp.ClientSession:
        self._session = aiohttp.ClientSession(headers=self.headers)

        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.close()


class KuCoinAPI():

    __API_URL = "https://api.kucoin.com/api/v1/orders"

    def __init__(self, api_key, api_secret, api_passphrase) -> None:
        self.__API_KEY: str = api_key
        self.__API_SECRET: str = api_secret
        self.__API_PASSPHRASE: str = api_passphrase

    async def market_buy(self, size: str, symbol: str):
        logger.info(f"{self.__API_KEY} buy {size} {symbol}")
        timestamp = int(time.time() * 1000)

        data = self._get_data(size=size, symbol=symbol)
    
        sign = str(timestamp) + 'POST' + '/api/v1/orders' + data

        async with _KuCoinContextManager(
            self.__API_KEY,
            self._passphrase,
            self._get_signature(sign),
            str(timestamp)
        ) as kucoin_session:
            async with kucoin_session.post(
                self.__API_URL,
                data=data
            ) as resp:
                return await resp.json()

    def _get_signature(self, sign: str) -> str:
        return base64.b64encode(
            hmac.new(
                self.__API_SECRET.encode('utf-8'),
                sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode("utf-8")

    @property
    def _passphrase(self):
        return base64.b64encode(
            hmac.new(
                self.__API_SECRET.encode('utf-8'),
                self.__API_PASSPHRASE.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode("utf-8")

    @staticmethod
    def _get_data(size: str, symbol: str) -> str:
        return json.dumps({"clientOid": "AAA", "side": "buy", "symbol": symbol.upper(), "type": "market", "size": size})
