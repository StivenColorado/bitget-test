import json
import time
import requests
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
from datetime import datetime

load_dotenv()

app = FastAPI(title="Bitget Futures Orders Extractor", version="1.0.0")

class ExtractRequest(BaseModel):
    symbols: List[str]

class ExtractResponse(BaseModel):
    success: bool
    data: List[Dict]
    duration_seconds: float
    total_orders: int

class ExtractFuturesRequest(BaseModel):
    symbols: Optional[List[str]] = None  # Si es None, obtiene todos los símbolos
    start_time: Optional[int] = None  # ms since epoch (opcional)
    end_time: Optional[int] = None    # ms since epoch (opcional)

def extract_futures_orders_local(symbols: Optional[List[str]] = None, 
                                start_time: Optional[int] = None, 
                                end_time: Optional[int] = None,
                                test_mode: bool = False) -> Dict:
    """Extract futures orders locally using ThreadPool"""
    start_time_exec = time.time()
    
    print("Initializing Bitget client...")
    client = BitgetClient(
        os.getenv('BITGET_API_KEY'),
        os.getenv('BITGET_SECRET_KEY'),
        os.getenv('BITGET_PASSPHRASE')
    )
    
    print(f"API credentials loaded: KEY={os.getenv('BITGET_API_KEY')[:10]}...")
    
    # Si no se proporcionan símbolos, obtener todos los símbolos de futuros
    if not symbols:
        print("Fetching all futures symbols...")
        symbols = client.get_futures_symbols()
        if not symbols:
            return {
                'success': False,
                'data': [],
                'duration_seconds': 0,
                'total_orders': 0,
                'error': 'Could not fetch futures symbols'
            }
        print(f"Found {len(symbols)} futures symbols")
    else:
        print(f"Using provided symbols: {symbols}")
    
    all_orders = []
    
    def fetch_symbol_futures_orders(symbol: str):
        try:
            print(f"Fetching futures orders for {symbol}...")
            orders = BitgetClient.fetch_all_futures_history_for_symbol(
                client=client,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                per_page=100,
                sleep_between=0.12
            )
            # Asegurar que cada orden tenga el símbolo
            for order in orders:
                order['symbol'] = symbol
            print(f"Fetched {len(orders)} orders for {symbol}")
            return orders
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return []
    
    # Selección de símbolos según modo de prueba
    if test_mode:
        selected_symbols = symbols[:5] if len(symbols) > 5 else symbols
        print(f"Test mode enabled. Using first {len(selected_symbols)} symbols: {selected_symbols}")
    else:
        selected_symbols = symbols
        print(f"Production mode. Using all {len(selected_symbols)} symbols")

    # Paralelismo según modo
    if test_mode:
        max_workers = min(len(selected_symbols), 3)
    else:
        max_workers = min(len(selected_symbols), 10)
    print(f"Starting parallel extraction with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_symbol_futures_orders, symbol): symbol 
                           for symbol in selected_symbols}
        
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                orders = future.result()
                if orders:
                    all_orders.extend(orders)
                    print(f"Added {len(orders)} orders from {symbol}. Total so far: {len(all_orders)}")
                else:
                    print(f"No orders found for {symbol}")
            except Exception as exc:
                print(f"Symbol {symbol} generated an exception: {exc}")
    
    # Deduplicar por orderId y ordenar cronológicamente
    seen = set()
    deduped_orders = []
    for order in all_orders:
        order_id = order.get('orderId')
        symbol = order.get('symbol')
        key = (symbol, order_id)
        if key not in seen:
            seen.add(key)
            deduped_orders.append(order)
    
    # Sort chronologically (by creation time) - más reciente primero
    deduped_orders.sort(key=lambda x: int(x.get('ctime') or x.get('cTime') or 0), reverse=True)
    
    duration = time.time() - start_time_exec
    
    print(f"Extraction completed: {len(deduped_orders)} unique orders from {len(selected_symbols)} symbols in {duration:.2f}s")
    
    # Guardar si tenemos órdenes (también en modo prueba)
    saved_file = None
    if deduped_orders:
        saved_file = save_orders_to_json(deduped_orders, duration, len(selected_symbols))
    else:
        print("No orders found to save")
    
    return {
        'success': True,
        'data': deduped_orders,
        'duration_seconds': round(duration, 2),
        'total_orders': len(deduped_orders),
        'processed_symbols': len(selected_symbols),
        'saved_file': saved_file
    }

def save_orders_to_json(orders: List[Dict], duration: float, symbol_count: int):
    """Save orders to a local JSON file"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Save in project root
        filename = f"bitget_futures_orders_{timestamp}.json"
        
        result = {
            "success": True,
            "extraction_timestamp": timestamp,
            "duration_seconds": round(duration, 2),
            "total_orders": len(orders),
            "processed_symbols": symbol_count,
            "data": orders
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Orders saved to: {os.path.abspath(filename)}")
        return filename
        
    except Exception as e:
        print(f"Error saving orders to JSON: {e}")
        return None

def extract_orders_aws(symbols: List[str]) -> Dict:
    """Extract orders using AWS Step Functions with MASSIVE PARALLEL execution"""
    print(f"Executing MASSIVE parallel extraction for {len(symbols)} symbols using Step Functions")
    
    try:
        # Step Functions client pointing to LocalStack
        sf_client = boto3.client(
            'stepfunctions',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id='test',
            aws_secret_access_key='test',
            endpoint_url='http://localhost:4566'
        )

        # Split symbols into batches for parallel processing
        BATCH_SIZE = 25  # Each worker processes 25 symbols
        symbol_batches = []
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            symbol_batches.append(batch)
        
        print(f"Created {len(symbol_batches)} batches of ~{BATCH_SIZE} symbols each")

        # Create input for Step Functions with parallel workers
        input_data = {
            'symbol_batches': symbol_batches,
            'use_fills': True,  # Use fills instead of orders (more likely to have data)
            'start_time': int((time.time() - (30 * 24 * 3600)) * 1000),  # Last 30 days
            'end_time': int(time.time() * 1000),
            'total_symbols': len(symbols),
            'execution_id': f"extraction_{int(time.time())}"
        }

        print(f"Step Functions input: {len(symbol_batches)} parallel workers")

        # Start Step Function execution
        response = sf_client.start_execution(
            stateMachineArn=os.getenv(
                'STATE_MACHINE_ARN',
                'arn:aws:states:us-east-1:000000000000:stateMachine:BitgetMassiveExtractionStateMachine'
            ),
            input=json.dumps(input_data)
        )

        execution_arn = response['executionArn']
        print(f"Started Step Function execution: {execution_arn}")

        # Wait for execution to complete with progress updates
        start_wait = time.time()
        last_status = None
        
        while True:
            exec_response = sf_client.describe_execution(executionArn=execution_arn)
            status = exec_response['status']
            
            if status != last_status:
                elapsed = time.time() - start_wait
                print(f"Step Function status: {status} (elapsed: {elapsed:.1f}s)")
                last_status = status

            if status == 'SUCCEEDED':
                result = json.loads(exec_response['output'])
                print(f"Step Function completed successfully!")
                break
            elif status == 'FAILED':
                error_details = exec_response.get('error', 'Unknown error')
                cause = exec_response.get('cause', '')
                raise Exception(f"Step Function failed: {error_details}. Cause: {cause}")
            elif status in ['TIMED_OUT', 'ABORTED']:
                raise Exception(f"Step Function {status.lower()}")
            
            time.sleep(2)  # Check every 2 seconds

        # Save result to S3
        s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id='test',
            aws_secret_access_key='test',
            endpoint_url='http://localhost:4566'
        )

        bucket_name = 'bitget-massive-results'
        
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except:
            print(f"Creating bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)

        # Save massive result JSON
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bitget_massive_extraction_{timestamp}.json"
        
        json_bytes = json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8')
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json_bytes,
            ContentType='application/json'
        )

        # Verify save
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        print(f"Files in bucket '{bucket_name}': {[obj['Key'] for obj in objects.get('Contents', [])]}")
        print(f"Massive result saved to LocalStack S3: s3://{bucket_name}/{filename}")

        # Add S3 location to result
        result['s3_location'] = f"s3://{bucket_name}/{filename}"
        result['extraction_method'] = 'step_functions_massive_parallel'

        return result

    except Exception as e:
        print(f"AWS extraction failed, falling back to local: {e}")
        # Fallback to local extraction
        return extract_futures_orders_local(symbols=symbols, test_mode=True)

@app.post("/extract/massive", response_model=ExtractResponse)
async def extract_massive_futures_orders(request: ExtractFuturesRequest):
    """
    MASSIVE parallel extraction of ALL futures orders using Step Functions
    This is the main endpoint for extracting ~17,000+ orders across all symbols
    """
    print("=== STARTING MASSIVE FUTURES EXTRACTION ===")
    
    start_time_total = time.time()
    
    # Initialize client to get all symbols if none provided
    client = BitgetClient(
        os.getenv('BITGET_API_KEY'),
        os.getenv('BITGET_SECRET_KEY'),
        os.getenv('BITGET_PASSPHRASE')
    )
    
    # Get all futures symbols if none provided
    if not request.symbols:
        print("Fetching ALL futures symbols for massive extraction...")
        symbols = client.get_futures_symbols()
        if not symbols:
            raise HTTPException(status_code=500, detail="Could not fetch futures symbols")
        print(f"Found {len(symbols)} futures symbols for massive extraction")
    else:
        symbols = request.symbols
        print(f"Using provided {len(symbols)} symbols")
    
    # Force AWS execution for massive extraction
    print(f"Starting MASSIVE AWS extraction for {len(symbols)} symbols...")
    
    # Estimate processing time
    estimated_batches = len(symbols) // 25 + (1 if len(symbols) % 25 else 0)
    estimated_time = estimated_batches * 2  # ~2 minutes per batch in parallel
    print(f"Estimated processing: {estimated_batches} parallel workers, ~{estimated_time} minutes")
    
    result = extract_orders_aws(symbols)
    
    total_duration = time.time() - start_time_total
    print(f"=== MASSIVE EXTRACTION COMPLETED in {total_duration:.1f}s ===")
    print(f"Total orders extracted: {result.get('total_orders', 0)}")
    
    return ExtractResponse(**result)

@app.post("/extract/futures", response_model=ExtractResponse)
async def extract_futures_orders(request: ExtractFuturesRequest):
    """Extract futures orders from Bitget for given symbols (or all symbols if none provided)"""
    
    print(f"Received request: symbols={request.symbols}, start_time={request.start_time}, end_time={request.end_time}")
    
    # Check if running in debug/local mode
    if os.getenv('DEBUG', 'true').lower() == 'true':
        result = extract_futures_orders_local(
            symbols=request.symbols,
            start_time=request.start_time,
            end_time=request.end_time
        )
    else:
        # Para AWS, necesitamos símbolos específicos
        if not request.symbols:
            # Obtener símbolos primero
            client = BitgetClient(
                os.getenv('BITGET_API_KEY'),
                os.getenv('BITGET_SECRET_KEY'),
                os.getenv('BITGET_PASSPHRASE')
            )
            request.symbols = client.get_futures_symbols()
        
        result = extract_orders_aws(request.symbols)
    
    return ExtractResponse(**result)

@app.post("/extract", response_model=ExtractResponse)
async def extract_orders(request: ExtractRequest):
    """Legacy endpoint - Extract orders from Bitget for given symbols"""
    
    if not request.symbols:
        raise HTTPException(status_code=400, detail="Symbols list cannot be empty")
    
    # Check if running in debug/local mode
    if os.getenv('DEBUG', 'false').lower() == 'true':
        result = extract_futures_orders_local(symbols=request.symbols)
    else:
        result = extract_orders_aws(request.symbols)
    
    return ExtractResponse(**result)

@app.get("/test/auth")
async def test_auth():
    """Test Bitget API authentication and basic connectivity"""
    try:
        client = BitgetClient(
            os.getenv('BITGET_API_KEY'),
            os.getenv('BITGET_SECRET_KEY'),
            os.getenv('BITGET_PASSPHRASE')
        )
        
        print("Testing basic API connectivity...")
        
        # Test 1: Get server time (no auth required)
        path = "/api/v2/public/time"
        headers = {'Content-Type': 'application/json'}
        url = f"https://api.bitget.com{path}"
        
        response = requests.get(url, headers=headers, timeout=30)
        server_time_data = response.json()
        
        print(f"Server time response: {server_time_data}")
        
        # Test 2: Get futures symbols (no auth required)
        symbols = client.get_futures_symbols()
        
        # Test 3: Try to get account info (requires auth)
        path = "/api/v2/mix/account/accounts"
        params = {"productType": "usdt-futures"}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = client._get_headers('GET', full_path)
        url = f"https://api.bitget.com{full_path}"
        
        auth_response = requests.get(url, headers=headers, timeout=30)
        auth_data = auth_response.json()
        
        print(f"Auth test response: {auth_data}")
        
        return {
            "success": True,
            "server_time": server_time_data,
            "symbols_count": len(symbols),
            "first_symbols": symbols[:5] if symbols else [],
            "auth_test": {
                "status_code": auth_response.status_code,
                "response_code": auth_data.get('code'),
                "message": auth_data.get('msg')
            }
        }
        
    except Exception as e:
        print(f"Auth test error: {e}")
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/test/orders/{symbol}")
async def test_orders_for_symbol(symbol: str):
    """Test fetching orders for a specific symbol with detailed logging"""
    try:
        client = BitgetClient(
            os.getenv('BITGET_API_KEY'),
            os.getenv('BITGET_SECRET_KEY'),
            os.getenv('BITGET_PASSPHRASE')
        )
        
        print(f"Testing orders for symbol: {symbol}")
        
        # Test direct API call
        result = client.get_futures_history_orders(symbol=symbol, limit=10)
        
        # Also test current orders (not historical)
        path = "/api/v2/mix/order/current-orders"
        params = {
            "symbol": symbol,
            "productType": "usdt-futures"
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = client._get_headers('GET', full_path)
        url = f"https://api.bitget.com{full_path}"
        
        current_response = requests.get(url, headers=headers, timeout=30)
        current_data = current_response.json()
        
        # Test fills/trades
        path = "/api/v2/mix/order/fills"
        params = {
            "symbol": symbol,
            "productType": "usdt-futures"
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_path = f"{path}?{query_string}"
        headers = client._get_headers('GET', full_path)
        url = f"https://api.bitget.com{full_path}"
        
        fills_response = requests.get(url, headers=headers, timeout=30)
        fills_data = fills_response.json()
        
        return {
            "success": True,
            "symbol": symbol,
            "history_orders": {
                "code": result.get('code'),
                "message": result.get('msg'),
                "data_type": type(result.get('data')).__name__,
                "order_count": len(result.get('data', {}).get('orderList', [])) if isinstance(result.get('data'), dict) else 0,
                "sample_data": result.get('data', {}).get('orderList', [])[:2] if isinstance(result.get('data'), dict) else []
            },
            "current_orders": {
                "code": current_data.get('code'),
                "message": current_data.get('msg'),
                "order_count": len(current_data.get('data', {}).get('orderList', [])) if isinstance(current_data.get('data'), dict) else 0
            },
            "fills": {
                "code": fills_data.get('code'),
                "message": fills_data.get('msg'),
                "fill_count": len(fills_data.get('data', {}).get('fillList', [])) if isinstance(fills_data.get('data'), dict) else 0,
                "sample_fills": fills_data.get('data', {}).get('fillList', [])[:2] if isinstance(fills_data.get('data'), dict) else []
            }
        }
        
    except Exception as e:
        print(f"Test orders error: {e}")
        return {
            "success": False,
            "error": str(e)
        }

@app.post("/extract/fills")
async def extract_futures_fills(request: ExtractFuturesRequest):
    """Extract futures fills/trades instead of orders - these are actual executed trades"""
    start_time_exec = time.time()
    
    client = BitgetClient(
        os.getenv('BITGET_API_KEY'),
        os.getenv('BITGET_SECRET_KEY'),
        os.getenv('BITGET_PASSPHRASE')
    )
    
    # Si no se proporcionan símbolos, obtener algunos símbolos principales
    if not request.symbols:
        symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "BCHUSDT", "LTCUSDT"]
    else:
        symbols = request.symbols
    
    print(f"Extracting fills for symbols: {symbols}")
    
    all_fills = []
    
    def fetch_symbol_fills(symbol: str):
        try:
            path = "/api/v2/mix/order/fills"
            params = {
                "symbol": symbol,
                "productType": "usdt-futures",
                "limit": 100
            }
            
            if request.start_time:
                params["startTime"] = request.start_time
            if request.end_time:
                params["endTime"] = request.end_time
            
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_path = f"{path}?{query_string}"
            headers = client._get_headers('GET', full_path)
            url = f"https://api.bitget.com{full_path}"
            
            print(f"Fetching fills for {symbol}...")
            response = requests.get(url, headers=headers, timeout=30)
            data = response.json()
            
            if data.get('code') == '00000':
                fills = data.get('data', {}).get('fillList', [])
                for fill in fills:
                    fill['symbol'] = symbol
                print(f"Found {len(fills)} fills for {symbol}")
                return fills
            else:
                print(f"Error for {symbol}: {data.get('msg')}")
                return []
                
        except Exception as e:
            print(f"Error fetching fills for {symbol}: {e}")
            return []
    
    # Ejecutar en paralelo
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_symbol = {executor.submit(fetch_symbol_fills, symbol): symbol 
                           for symbol in symbols}
        
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                fills = future.result()
                if fills:
                    all_fills.extend(fills)
                    print(f"Added {len(fills)} fills from {symbol}")
            except Exception as exc:
                print(f"Symbol {symbol} generated an exception: {exc}")
    
    # Ordenar por timestamp
    all_fills.sort(key=lambda x: int(x.get('cTime', 0)), reverse=True)
    
    duration = time.time() - start_time_exec
    
    if all_fills:
        save_orders_to_json(all_fills, duration, len(symbols))
    
    return {
        "success": True,
        "data": all_fills,
        "duration_seconds": round(duration, 2),
        "total_orders": len(all_fills),
        "processed_symbols": len(symbols)
    }

@app.get("/symbols/futures")
async def get_futures_symbols():
    """Get all available futures symbols"""
    try:
        client = BitgetClient(
            os.getenv('BITGET_API_KEY'),
            os.getenv('BITGET_SECRET_KEY'),
            os.getenv('BITGET_PASSPHRASE')
        )
        symbols = client.get_futures_symbols()
        return {
            "success": True,
            "symbols": symbols,
            "total_symbols": len(symbols)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching symbols: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "bitget-futures-orders-extractor"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)