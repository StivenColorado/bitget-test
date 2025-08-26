# Cliente pequeño para Bitget (firma + helpers)

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
    
    def get_orders(self, symbol: str, status: str = 'all') -> List[Dict]:
        """
        Get orders for a specific symbol using Bitget API v1
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT' or 'BTCUSDT_UMCBL')
            status: Order status to filter by. Options: 'all' (default), 'new', 'partially_filled', 'filled', 'cancelled'
        """
        # Remove any suffix like '_UMCBL' from the symbol
        clean_symbol = symbol.split('_')[0]
        path = "/api/spot/v1/trade/orders"
        
        # Map status to API values
        status_map = {
            'all': 'all',
            'new': 'new',
            'partially_filled': 'partially_filled',
            'filled': 'filled',
            'cancelled': 'cancelled'
        }
        
        params = {
            'symbol': clean_symbol,  # Use the cleaned symbol
            'status': status_map.get(status.lower(), 'all'),
            'limit': 100
        }
        
        full_path = f"{path}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        headers = self._get_headers('GET', full_path)
        url = f"{self.base_url}{full_path}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') != '00000':
                error_msg = f"API Error for {symbol}: {data.get('msg')}"
                logger.error(error_msg)
                print(error_msg)
                return []
            
            orders = data.get('data', [])
            if not orders:
                logger.info(f"No orders found for {symbol} with status '{status}'. Full response: {json.dumps(data, indent=2)}")
                print(f"No orders found for {symbol} with status '{status}'. Check logs for full response.")
            
            return orders
            
        except Exception as e:
            error_msg = f"Error fetching orders for {symbol}: {str(e)}"
            logger.error(error_msg)
            print(error_msg)
            return []

    def fetch_all_history_orders_for_symbol(client, symbol, start_time=None, end_time=None, per_page=100, max_pages=None, sleep_between=0.15):
        """
        Obtiene *todas* las órdenes históricas para `symbol` usando paginación.
        - client: instancia de BitgetClient con método `history_orders(symbol, startTime, endTime, limit, cursor)`
        (si no existe, añade un método en bitget_client que llame a /api/v2/spot/trade/history-orders o al endpoint correcto).
        - per_page: máximo 100 (limit de la API).
        - sleep_between: segundos entre peticiones (para evitar rate limits).
        - max_pages: opcional, límite de páginas para proteger contra loops infinitos.
        Retorna: lista de orders (raw).
        """

        all_items = []
        cursor = None
        page = 0
        while True:
            page += 1
            try:
                # Llamada al cliente. Ajusta según la firma de tu client (aquí asumimos un método "history_orders")
                resp = client.history_orders(symbol=symbol, start_time=start_time, end_time=end_time, limit=per_page, cursor=cursor)
            except Exception as e:
                logger.exception("Error calling history_orders: %s", e)
                # retry/backoff básico
                time.sleep(1.0)
                continue

            # Normalizar estructura de la respuesta (Bitget suele devolver {'code':..., 'data': ...})
            data = resp.get('data') if isinstance(resp, dict) else None

            # Detectar items y cursor según variantes del API
            items = []
            next_cursor = None

            if isinstance(data, dict):
                # Ejemplos: data puede contener keys como 'data' (lista), 'result', 'list', 'rows'
                items = data.get('data') or data.get('result') or data.get('list') or data.get('rows') or []
                next_cursor = data.get('cursor') or data.get('nextCursor') or data.get('next') or None
            elif isinstance(data, list):
                items = data
                next_cursor = None
            else:
                # fallback: intentar buscar 'data' directo en resp
                if isinstance(resp, dict) and 'cursor' in resp:
                    next_cursor = resp.get('cursor')
                items = resp.get('data') if isinstance(resp.get('data'), list) else []

            # Agregar items
            if items:
                all_items.extend(items)

            logger.info("Fetched page %s, got %d items, next_cursor=%s", page, len(items), str(next_cursor))

            # Control de terminación
            if not next_cursor or (max_pages and page >= max_pages):
                break

            # Preparar siguiente iteración
            cursor = next_cursor
            time.sleep(sleep_between)

        return all_items

    def history_orders(self, symbol: str, start_time: Optional[int] = None, end_time: Optional[int] = None,
                      limit: int = 100, cursor: Optional[str] = None) -> dict:
        """
        Fetch historical orders for a symbol with optional time window and pagination cursor.
        Returns raw API response (dict).
        """
        path = "/api/v2/spot/trade/history-orders"
        # /api/v2/spot/trade/history-orders
        params = {
            "symbol": symbol,
            "limit": limit
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if cursor:
            params["cursor"] = cursor

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = self._get_headers('GET', full_path)
        url = f"{self.base_url}{full_path}"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error in history_orders for {symbol}: {e}")
            return {"code": "error", "msg": str(e), "data": []}