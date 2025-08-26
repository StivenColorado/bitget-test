import json
import logging
import os
import time
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda collector que recibe resultados de todos los workers y genera el archivo JSON final
    
    Input esperado (lista de resultados de workers):
    [
        {
            "symbol": "BTCUSDT",
            "orders": [...],
            "totalOrders": 150,
            "processingTime": 2.5,
            "success": true
        },
        ...
    ]
    
    Output:
    {
        "success": true,
        "data": [...],  # Todas las órdenes unificadas
        "duration_seconds": 45.2,
        "total_orders": 17000,
        "processed_symbols": 150,
        "failed_symbols": 2,
        "s3_location": "s3://bitget-results/futures_orders_20250826_143022.json"
    }
    """
    
    start_time_collector = time.time()
    
    try:
        logger.info(f"Collector received {len(event)} worker results")
        
        all_orders = []
        processed_symbols = 0
        failed_symbols = 0
        processing_stats = []
        
        # Procesar resultados de cada worker
        for worker_result in event:
            if isinstance(worker_result, dict) and 'body' in worker_result:
                result_data = worker_result['body']
            else:
                result_data = worker_result
            
            symbol = result_data.get('symbol', 'unknown')
            success = result_data.get('success', False)
            
            if success and result_data.get('orders'):
                orders = result_data['orders']
                all_orders.extend(orders)
                processed_symbols += 1
                
                processing_stats.append({
                    'symbol': symbol,
                    'order_count': len(orders),
                    'processing_time': result_data.get('processingTime', 0)
                })
                
                logger.info(f"Added {len(orders)} orders from {symbol}")
            else:
                failed_symbols += 1
                logger.warning(f"Failed to process {symbol}: {result_data.get('error', 'Unknown error')}")
        
        # Deduplicar órdenes por orderId y symbol
        seen = set()
        deduped_orders = []
        for order in all_orders:
            order_id = order.get('orderId') or order.get('order_id') or order.get('id')
            symbol = order.get('symbol')
            key = (symbol, order_id)
            
            if key not in seen:
                seen.add(key)
                deduped_orders.append(order)
        
        logger.info(f"Deduplicated: {len(all_orders)} -> {len(deduped_orders)} orders")
        
        # Ordenar cronológicamente (más reciente primero)
        def get_timestamp(order):
            return int(order.get('ctime') or order.get('cTime') or 
                      order.get('timestamp') or order.get('createTime') or 0)
        
        deduped_orders.sort(key=get_timestamp, reverse=True)
        
        # Calcular duración total
        duration_seconds = time.time() - start_time_collector
        
        # Crear resultado final
        final_result = {
            "success": True,
            "data": deduped_orders,
            "duration_seconds": round(duration_seconds, 2),
            "total_orders": len(deduped_orders),
            "processed_symbols": processed_symbols,
            "failed_symbols": failed_symbols,
            "processing_stats": processing_stats,
            "execution_timestamp": int(time.time() * 1000),
            "metadata": {
                "total_symbols_attempted": len(event),
                "avg_orders_per_symbol": round(len(deduped_orders) / max(processed_symbols, 1), 2),
                "total_processing_time": sum(stat['processing_time'] for stat in processing_stats)
            }
        }
        
        # Guardar resultado en S3
        s3_location = save_to_s3(final_result)
        final_result['s3_location'] = s3_location
        
        logger.info(f"Collector completed: {len(deduped_orders)} total orders from {processed_symbols} symbols")
        logger.info(f"Result saved to: {s3_location}")
        
        return final_result
        
    except Exception as e:
        duration_seconds = time.time() - start_time_collector
        error_msg = f"Error in collector: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "success": False,
            "data": [],
            "duration_seconds": round(duration_seconds, 2),
            "total_orders": 0,
            "processed_symbols": 0,
            "failed_symbols": len(event) if isinstance(event, list) else 1,
            "error": str(e)
        }

def save_to_s3(result_data):
    """Guarda el resultado en S3 y retorna la ubicación"""
    try:
        # Cliente S3 para LocalStack
        s3_client = boto3.client(
            's3',
            region_name=os.environ.get('AWS_REGION', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
            endpoint_url=os.environ.get('S3_ENDPOINT_URL', 'http://localhost:4566')
        )
        
        bucket_name = 'bitget-results'
        
        # Crear bucket si no existe
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except:
            logger.info(f"Creating bucket {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
        
        # Generar nombre de archivo único
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"futures_orders_{timestamp}.json"
        
        # Guardar archivo
        json_content = json.dumps(result_data, indent=2, default=str)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        s3_location = f"s3://{bucket_name}/{filename}"
        logger.info(f"Successfully saved result to {s3_location}")
        
        return s3_location
        
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        return f"Error saving to S3: {str(e)}"