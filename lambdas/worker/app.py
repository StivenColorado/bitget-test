import json
import logging
import os
import time
from typing import List, Dict, Optional
from lambdas.bitget_client import BitgetClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda worker que procesa un batch de símbolos en paralelo
    
    Input esperado:
    {
        "symbols": ["BTCUSDT", "ETHUSDT", "XRPUSDT", ...],
        "start_time": 1640995200000,  # opcional
        "end_time": 1672531200000,    # opcional
        "worker_id": "worker-001",
        "use_fills": true             # true para fills, false para orders
    }
    
    Output:
    {
        "success": true,
        "worker_id": "worker-001",
        "processed_symbols": 25,
        "total_orders": 1247,
        "orders": [...],  # Todas las órdenes/fills
        "processing_time": 45.2,
        "failed_symbols": ["SYMBOL1"],
        "symbol_stats": {
            "BTCUSDT": {"orders": 150, "time": 2.3},
            "ETHUSDT": {"orders": 89, "time": 1.8},
            ...
        }
    }
    """
    
    start_time_worker = time.time()
    worker_id = event.get('worker_id', f'worker-{int(time.time())}')
    
    try:
        logger.info(f"Worker {worker_id} starting processing...")
        
        # Extraer parámetros
        symbols = event.get('symbols', [])
        start_time = event.get('start_time')
        end_time = event.get('end_time')
        use_fills = event.get('use_fills', True)  # Default to fills (more likely to have data)
        
        if not symbols:
            return {
                "success": False,
                "worker_id": worker_id,
                "error": "No symbols provided",
                "processed_symbols": 0,
                "total_orders": 0,
                "orders": [],
                "processing_time": 0
            }
        
        logger.info(f"Worker {worker_id} processing {len(symbols)} symbols with use_fills={use_fills}")
        
        # Inicializar cliente Bitget
        client = BitgetClient(
            api_key=os.environ.get('BITGET_API_KEY'),
            secret_key=os.environ.get('BITGET_SECRET_KEY'),
            passphrase=os.environ.get('BITGET_PASSPHRASE')
        )
        
        all_orders = []
        failed_symbols = []
        symbol_stats = {}
        
        # Procesar cada símbolo secuencialmente (el paralelismo está a nivel de workers)
        for symbol in symbols:
            symbol_start = time.time()
            try:
                logger.info(f"Worker {worker_id} processing symbol {symbol}")
                
                # Obtener órdenes/fills para el símbolo
                orders = BitgetClient.fetch_all_futures_history_for_symbol(
                    client=client,
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    per_page=100,
                    sleep_between=0.12,
                    use_fills=use_fills
                )
                
                # Asegurar que cada orden tenga el símbolo
                for order in orders:
                    order['symbol'] = symbol
                    order['worker_id'] = worker_id
                    order['data_type'] = 'fill' if use_fills else 'order'
                
                all_orders.extend(orders)
                
                processing_time = time.time() - symbol_start
                symbol_stats[symbol] = {
                    "orders": len(orders),
                    "time": round(processing_time, 2)
                }
                
                logger.info(f"Worker {worker_id} - {symbol}: {len(orders)} items in {processing_time:.2f}s")
                
            except Exception as e:
                processing_time = time.time() - symbol_start
                error_msg = f"Error processing {symbol}: {str(e)}"
                logger.error(error_msg)
                
                failed_symbols.append(symbol)
                symbol_stats[symbol] = {
                    "orders": 0,
                    "time": round(processing_time, 2),
                    "error": str(e)
                }
        
        # Deduplicar dentro del worker
        seen = set()
        deduped_orders = []
        for order in all_orders:
            # Crear clave única basada en múltiples campos
            order_id = (order.get('orderId') or order.get('tradeId') or 
                       order.get('fillId') or order.get('id'))
            symbol = order.get('symbol')
            timestamp = order.get('cTime') or order.get('timestamp')
            
            key = (symbol, order_id, timestamp)
            if key not in seen:
                seen.add(key)
                deduped_orders.append(order)
        
        total_processing_time = time.time() - start_time_worker
        
        logger.info(f"Worker {worker_id} completed: {len(deduped_orders)} unique items from {len(symbols)} symbols")
        
        return {
            "success": True,
            "worker_id": worker_id,
            "processed_symbols": len(symbols),
            "failed_symbols": failed_symbols,
            "total_orders": len(deduped_orders),
            "orders": deduped_orders,
            "processing_time": round(total_processing_time, 2),
            "symbol_stats": symbol_stats,
            "use_fills": use_fills,
            "execution_timestamp": int(time.time() * 1000)
        }
        
    except Exception as e:
        total_processing_time = time.time() - start_time_worker
        error_msg = f"Worker {worker_id} failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "success": False,
            "worker_id": worker_id,
            "error": str(e),
            "processed_symbols": 0,
            "total_orders": 0,
            "orders": [],
            "processing_time": round(total_processing_time, 2)
        }