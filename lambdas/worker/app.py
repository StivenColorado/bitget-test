# Handler: recibe { "symbol": "BTCUSDT" } -> consulta Bitget y devuelve órdenes

import json
import os
from lambdas.bitget_client import BitgetClient

def lambda_handler(event, context):
    """Worker Lambda: Extract orders for a single symbol"""
    
    symbol = event.get('symbol')
    if not symbol:
        return {'error': 'No symbol provided'}
    
    try:
        client = BitgetClient(
            api_key=os.environ['BITGET_API_KEY'],
            secret_key=os.environ['BITGET_SECRET_KEY'],
            passphrase=os.environ['BITGET_PASSPHRASE']
        )
        
        orders = client.get_orders(symbol)
        
        # Add symbol to each order
        for order in orders:
            order['symbol'] = symbol
        
        return {
            'symbol': symbol,
            'orders': orders,
            'count': len(orders)
        }
        
    except Exception as e:
        return {
            'symbol': symbol,
            'error': str(e),
            'orders': [],
            'count': 0
        }