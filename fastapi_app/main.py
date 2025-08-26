#FastApi endpoint /extract -> coordina local o invoca step functions
import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import boto3
from lambdas.bitget_client import BitgetClient
import os
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

app = FastAPI(title="Bitget Orders Extractor", version="1.0.0")

class ExtractRequest(BaseModel):
    symbols: List[str]

class ExtractResponse(BaseModel):
    success: bool
    data: List[Dict]
    duration_seconds: float
    total_orders: int

def extract_orders_local(symbols: List[str]) -> Dict:
    """Extract orders locally using ThreadPool"""
    start_time = time.time()
    
    client = BitgetClient(
    os.getenv('BITGET_API_KEY'),
    os.getenv('BITGET_SECRET_KEY'),
    os.getenv('BITGET_PASSPHRASE')
)
    
    all_orders = []
    
    def fetch_symbol_orders(symbol: str):
        try:
            orders = client.get_orders(symbol)
            for order in orders:
                order['symbol'] = symbol
            return orders
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return []
    
    # Parallel execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_symbol_orders, symbol) for symbol in symbols]
        for future in futures:
            all_orders.extend(future.result())
    
    # Sort chronologically (by creation time)
    all_orders.sort(key=lambda x: int(x.get('cTime', 0)), reverse=True)
    
    duration = time.time() - start_time
    
    return {
        'success': True,
        'data': all_orders,
        'duration_seconds': round(duration, 2),
        'total_orders': len(all_orders)
    }

def extract_orders_aws(symbols: List[str]) -> Dict:
    print(f"ejecutando extract_orders_aws con symbols: {symbols}")
    """Extract orders using AWS Step Functions (LocalStack) and save result to LocalStack S3"""
    try:
        # Step Functions client apuntando a LocalStack
        sf_client = boto3.client(
            'stepfunctions',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id='test',
            aws_secret_access_key='test',
            endpoint_url='http://localhost:4566'
        )

        input_data = {
            'symbols': symbols,
            'startTime': time.time()
        }

        # Inicia ejecución de Step Function
        response = sf_client.start_execution(
            stateMachineArn=os.getenv(
                'STATE_MACHINE_ARN',
                'arn:aws:states:us-east-1:000000000000:stateMachine:MyStateMachine'
            ),
            input=json.dumps(input_data)
        )

        execution_arn = response['executionArn']

        # Espera simulada a que la ejecución termine
        while True:
            exec_response = sf_client.describe_execution(executionArn=execution_arn)
            status = exec_response['status']

            if status == 'SUCCEEDED':
                result = json.loads(exec_response['output'])
                break
            elif status == 'FAILED':
                raise Exception(f"Execution failed: {exec_response.get('error')}")
            time.sleep(1)

        # Guardar resultado en S3 de LocalStack
        s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id='test',
            aws_secret_access_key='test',
            endpoint_url='http://localhost:4566'
        )

        bucket_name = 'bitget-results'
        # Crear bucket si no existe
        existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
        if bucket_name not in existing_buckets:
            print(f"Bucket '{bucket_name}' no existe, creando...")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            print(f"Bucket '{bucket_name}' ya existe.")

        # Guardar JSON (asegura que Body sea bytes)
        json_bytes = json.dumps(result).encode('utf-8')
        put_resp = s3_client.put_object(
            Bucket=bucket_name,
            Key='resultado.json',
            Body=json_bytes
        )
        print(f"put_object response: {put_resp}")

        # Verifica que el archivo se guardó correctamente
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        print(f"Objetos en bucket '{bucket_name}': {objects.get('Contents', [])}")

        print(f"Result saved to LocalStack S3 bucket '{bucket_name}' as 'resultado.json'")

        return result

    except Exception as e:
        print(f"Error al guardar en S3: {e}")
        raise HTTPException(status_code=500, detail=f"AWS execution failed: {str(e)}")


# Modelo de request opcionalmente con rango temporal (timestamps en ms)
class ExtractHistoryRequest(BaseModel):
    symbols: List[str]
    start_time: Optional[int] = None  # ms since epoch (opcional)
    end_time: Optional[int] = None    # ms since epoch (opcional)

# Respuesta simple (igual formato que tu extract_orders_local)
class ExtractHistoryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    duration_seconds: float
    total_orders: int


@app.post("/extract/history", response_model=ExtractHistoryResponse)
async def extract_orders_history(request: ExtractHistoryRequest):
    """Extract full historical orders for the provided symbols (paginated)."""
    if not request.symbols:
        raise HTTPException(status_code=400, detail="Symbols list cannot be empty")

    start_time_exec = time.time()

    # Inicializa cliente con variables de entorno (ajusta nombres si usas otros)
    client = BitgetClient(
        api_key=os.getenv('BITGET_API_KEY'),
        secret_key=os.getenv('BITGET_API_SECRET') or os.getenv('BITGET_SECRET_KEY'),
        passphrase=os.getenv('BITGET_PASSPHRASE')
    )

    all_orders = []
    errors = []

    def fetch_for_symbol(symbol: str):
        try:
            # Llama a la función que implementa paginación/cursor y ventanas temporales
            orders = BitgetClient.fetch_all_history_orders_for_symbol(
                client=client,
                symbol=symbol,
                start_time=request.start_time,
                end_time=request.end_time,
                per_page=100,
                sleep_between=0.12
            )
            # Aseguramos que cada order tenga el symbol
            for o in orders:
                if 'symbol' not in o or not o['symbol']:
                    o['symbol'] = symbol
            return orders
        except Exception as e:
            # Devuelve error por símbolo para debug
            return {"__error__": str(e), "__symbol__": symbol}

    # Ejecutar en paralelo (ajusta max_workers según tu caso)
    max_workers = min(len(request.symbols), 10)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_for_symbol, s): s for s in request.symbols}
        for fut in as_completed(futures):
            res = fut.result()
            if isinstance(res, dict) and res.get("__error__"):
                errors.append(res)
            else:
                all_orders.extend(res)

    # Deduplicar por orderId si existe, y ordenar cronológicamente (más reciente primero)
    seen = set()
    deduped = []
    for o in all_orders:
        oid = o.get('orderId') or o.get('order_id') or o.get('id') or None
        key = (o.get('symbol'), oid)
        if key not in seen:
            seen.add(key)
            deduped.append(o)

    # Normaliza campo timestamp para ordenar (busca variantes y usa 0 por defecto)
    def _ts(item):
        return int(item.get('timestamp') or item.get('ctime') or item.get('cTime') or
                   item.get('createTime') or item.get('lastUpdateTime') or 0)

    deduped.sort(key=_ts, reverse=True)  # reverse=True -> más reciente primero

    duration = time.time() - start_time_exec

    return {
        'success': True,
        'data': deduped,
        'duration_seconds': round(duration, 2),
        'total_orders': len(deduped)
    }

@app.post("/extract", response_model=ExtractResponse)
async def extract_orders(request: ExtractRequest):
    """Extract orders from Bitget for given symbols"""
    
    if not request.symbols:
        raise HTTPException(status_code=400, detail="Symbols list cannot be empty")
    
    # Check if running in debug/local mode
    if os.getenv('DEBUG', 'false').lower() == 'true':
        result = extract_orders_local(request.symbols)
    else:
        result = extract_orders_aws(request.symbols)
    
    return ExtractResponse(**result)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "bitget-orders-extractor"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)