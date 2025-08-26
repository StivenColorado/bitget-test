import json
import logging
import os
import sys
import time

# Add lambdas directory to path for imports
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

from bitget_client import BitgetClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda coordinador que obtiene todos los símbolos de futures y prepara la ejecución en paralelo
    
    Input esperado:
    {
        "symbols": ["BTCUSDT", "ETHUSDT"] (opcional, si no se proporciona obtiene todos)
        "startTime": timestamp_ms (opcional),
        "endTime": timestamp_ms (opcional)
    }
    
    Output:
    {
        "symbols": ["BTCUSDT", "ETHUSDT", ...],
        "startTime": timestamp_ms,
        "endTime": timestamp_ms,
        "totalSymbols": 150
    }
    """
    
    try:
        logger.info(f"Coordinator received event: {json.dumps(event)}")
        
        # Inicializar cliente Bitget
        client = BitgetClient(
            api_key=os.environ['BITGET_API_KEY'],
            secret_key=os.environ['BITGET_SECRET_KEY'],
            passphrase=os.environ['BITGET_PASSPHRASE']
        )
        
        # Obtener símbolos
        if 'symbols' in event and event['symbols']:
            # Usar símbolos proporcionados
            symbols = event['symbols']
            logger.info(f"Using provided symbols: {len(symbols)} symbols")
        else:
            # Obtener todos los símbolos de futures activos
            logger.info("Fetching all active futures symbols...")
            symbols = client.get_futures_symbols()
            logger.info(f"Retrieved {len(symbols)} active futures symbols")
        
        if not symbols:
            raise Exception("No symbols found or provided")
        
        # Preparar parámetros temporales
        start_time = event.get('startTime')
        end_time = event.get('endTime')
        
        # Si no se proporciona startTime, usar 30 días atrás por defecto
        if not start_time:
            # 30 días = 30 * 24 * 60 * 60 * 1000 ms
            start_time = int((time.time() - (30 * 24 * 60 * 60)) * 1000)
        
        result = {
            "symbols": symbols,
            "startTime": start_time,
            "endTime": end_time,
            "totalSymbols": len(symbols),
            "coordinatorTimestamp": int(time.time() * 1000)
        }
        
        logger.info(f"Coordinator result: {len(symbols)} symbols prepared for parallel processing")
        
        return {
            'statusCode': 200,
            'body': result
        }
        
    except Exception as e:
        logger.error(f"Error in coordinator: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'symbols': [],
                'totalSymbols': 0
            }
        }