# Cliente pequeño para Bitget (firma + helpers) - FIXED FOR FUTURES

import hashlib
import hmac
import base64
import time
import json
import requests
from typing import List, Dict, Optional
import logging

logger = logging.getLogger("bitget_pagination")
logger.setLevel(logging.INFO)

class BitgetClient:
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.base_url = "https://api.bitget.com"
    
    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """Generate signature for Bitget API"""
        message = timestamp + method + path + body
        signature = base64.b64encode(
            hmac.new(
                self.secret_key.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        return signature
    
    def _get_headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Generate headers for API requests"""
        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, method, path, body)
        
        return {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
    
    def get_futures_symbols(self) -> List[str]:
        """Get all active futures symbols"""
        try:
            path = "/api/v2/mix/market/contracts"
            params = {"productType": "USDT-FUTURES"}  # Corrected case
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_path = f"{path}?{query_string}"
            
            headers = self._get_headers('GET', full_path)
            url = f"{self.base_url}{full_path}"
            
            print(f"Fetching symbols from: {url}")
            
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Response status: {response.status_code}")
            
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') != '00000':
                logger.error(f"API Error getting symbols: {data.get('msg')}")
                return []
            
            contracts = data.get('data', [])
            symbols = [contract['symbol'] for contract in contracts if contract.get('symbol')]
            
            logger.info(f"Found {len(symbols)} futures symbols")
            return symbols
            
        except Exception as e:
            logger.error(f"Error fetching futures symbols: {str(e)}")
            return []

    def get_futures_history_orders(self, symbol: str, start_time: Optional[int] = None, 
                                 end_time: Optional[int] = None, limit: int = 100, 
                                 end_id: Optional[str] = None) -> dict:
        """
        Get futures historical orders for a specific symbol
        Uses: /api/v2/mix/order/history-orders
        """
        path = "/api/v2/mix/order/history-orders"
        params = {
            "symbol": symbol.strip(),  # Remove any whitespace
            "productType": "USDT-FUTURES",  # Corrected case
            "limit": min(100, max(1, int(limit)))
        }
        
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if end_id:
            params["endId"] = end_id

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = self._get_headers('GET', full_path)
        url = f"{self.base_url}{full_path}"

        try:
            print(f"Fetching orders for {symbol}: {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            print(f"Response status for {symbol}: {resp.status_code}")
            
            if resp.status_code != 200:
                print(f"Error response for {symbol}: {resp.text}")
            
            resp.raise_for_status()
            data = resp.json()
            
            print(f"API response code for {symbol}: {data.get('code')}")
            if data.get('code') != '00000':
                print(f"API error for {symbol}: {data.get('msg')}")
            else:
                orders_count = len(data.get('data', {}).get('orderList', [])) if isinstance(data.get('data'), dict) else 0
                print(f"Found {orders_count} orders for {symbol}")
            
            return data
        except Exception as e:
            logger.error(f"futures_history_orders error for {symbol}: {e}")
            print(f"Exception for {symbol}: {e}")
            return {"code": "error", "msg": str(e), "data": []}

    def get_futures_fills(self, symbol: str, start_time: Optional[int] = None, 
                         end_time: Optional[int] = None, limit: int = 100, 
                         end_id: Optional[str] = None) -> dict:
        """
        Get futures fills/trades (executed orders) - this is more likely to have data
        Uses: /api/v2/mix/order/fills
        """
        path = "/api/v2/mix/order/fills"
        params = {
            "symbol": symbol.strip(),
            "productType": "USDT-FUTURES",
            "limit": min(100, max(1, int(limit)))
        }
        
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if end_id:
            params["endId"] = end_id

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = self._get_headers('GET', full_path)
        url = f"{self.base_url}{full_path}"

        try:
            print(f"Fetching fills for {symbol}: {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            
            resp.raise_for_status()
            data = resp.json()
            
            if data.get('code') != '00000':
                print(f"API error for {symbol}: {data.get('msg')}")
                return {"code": "error", "msg": data.get('msg'), "data": {"fillList": []}}
            else:
                fills_count = len(data.get('data', {}).get('fillList', []))
                print(f"Found {fills_count} fills for {symbol}")
            
            return data
        except Exception as e:
            logger.error(f"futures_fills error for {symbol}: {e}")
            return {"code": "error", "msg": str(e), "data": {"fillList": []}}

    @staticmethod
    def fetch_all_futures_history_for_symbol(client, symbol: str, start_time: Optional[int] = None,
                                            end_time: Optional[int] = None, per_page: int = 100,
                                            max_pages: Optional[int] = None, sleep_between: float = 0.12,
                                            use_fills: bool = True) -> List[Dict]:
        """
        High-level paginator for futures historical data.
        use_fills=True: fetch fills/trades (more likely to have data)
        use_fills=False: fetch orders
        """
        all_items: List[Dict] = []
        end_id: Optional[str] = None
        page = 0

        while True:
            page += 1
            try:
                if use_fills:
                    resp = client.get_futures_fills(
                        symbol=symbol, 
                        start_time=start_time, 
                        end_time=end_time,
                        limit=per_page, 
                        end_id=end_id
                    )
                else:
                    resp = client.get_futures_history_orders(
                        symbol=symbol, 
                        start_time=start_time, 
                        end_time=end_time,
                        limit=per_page, 
                        end_id=end_id
                    )
            except Exception as e:
                logger.exception("Error calling API: %s", e)
                time.sleep(1.0)
                continue

            # Check API response
            if resp.get('code') != '00000':
                logger.error(f"API Error for {symbol}: {resp.get('msg')}")
                break

            # Get data from response
            data = resp.get('data', {})
            if use_fills:
                items = data.get('fillList', []) if isinstance(data, dict) else []
            else:
                items = data.get('orderList', []) if isinstance(data, dict) else []
            
            if not items:
                logger.info(f"No more items for {symbol} on page {page}")
                break

            all_items.extend(items)
            
            # Check if there are more pages
            next_flag = data.get('nextFlag', False) if isinstance(data, dict) else False
            if not next_flag or len(items) < per_page:
                logger.info(f"No more pages for {symbol} (nextFlag: {next_flag})")
                break

            # Use appropriate ID field for pagination
            if use_fills:
                end_id = items[-1].get('tradeId') or items[-1].get('fillId')
            else:
                end_id = items[-1].get('orderId')
                
            if not end_id:
                logger.warning(f"No ID found in last item for {symbol}")
                break

            logger.info(f"Fetched page {page} for {symbol}, got {len(items)} items, total so far: {len(all_items)}")

            if max_pages and page >= max_pages:
                break

            time.sleep(sleep_between)

        logger.info(f"Total fetched for {symbol}: {len(all_items)} items")
        return all_items