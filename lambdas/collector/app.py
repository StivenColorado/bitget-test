# Handler: une resultados, ordena cronológicamente y añade duration

import json
import time

def lambda_handler(event, context):
    """Collector Lambda: Combine and sort all results"""
    
    start_time = event.get('startTime', time.time())
    results = event.get('results', [])
    
    all_orders = []
    total_processed = 0
    errors = []
    
    # Collect all orders from worker results
    for result in results:
        if 'error' in result:
            errors.append({
                'symbol': result.get('symbol'),
                'error': result['error']
            })
        else:
            all_orders.extend(result.get('orders', []))
            total_processed += result.get('count', 0)
    
    # Sort chronologically (newest first)
    all_orders.sort(key=lambda x: int(x.get('cTime', 0)), reverse=True)
    
    duration = time.time() - start_time
    
    return {
        'success': True,
        'data': all_orders,
        'duration_seconds': round(duration, 2),
        'total_orders': len(all_orders),
        'symbols_processed': len(results),
        'errors': errors
    }