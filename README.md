# Bitget Orders Extractor - Documentación

## Descripción General

Este proyecto es un sistema para extraer y procesar datos de órdenes del exchange de criptomonedas Bitget. La arquitectura está diseñada para funcionar tanto localmente como en la nube de AWS, utilizando funciones Lambda para el procesamiento distribuido.

## Estructura del Proyecto

```
bitget-test/
├── fastapi_app/           # Aplicación FastAPI para la API
│   └── main.py           # Endpoints y lógica principal
├── lambdas/              # Funciones Lambda para procesamiento distribuido
│   ├── collector/        # Recolecta y combina resultados
│   ├── coordinator/      # Coordina la ejecución de workers
│   ├── worker/           # Procesa órdenes para símbolos específicos
│   └── bitget_client.py  # Cliente para interactuar con la API de Bitget
├── docker-compose.yml    # Configuración de contenedores
├── requirements.txt      # Dependencias de Python
└── template.yaml         # Plantilla AWS SAM
```

## Componentes Principales

### 1. API FastAPI

**Archivo**: `fastapi_app/main.py`

Endpoints principales:
- `POST /extract`: Extrae órdenes para una lista de símbolos
- `POST /extract/history`: Extrae historial de órdenes con filtros de tiempo
- `GET /health`: Verifica el estado del servicio

### 2. Funciones Lambda

#### a) Collector (`lambdas/collector/app.py`)
- **Función**: Recopila y combina resultados de múltiples workers
- **Entrada**: Resultados de workers
- **Salida**: Lista ordenada de órdenes con metadatos

#### b) Coordinator (`lambdas/coordinator/app.py`)
- **Función**: Coordina la ejecución paralela de workers
- **Responsabilidades**:
  - Divide la carga de trabajo
  - Invoca workers en paralelo
  - Maneja errores y timeouts

#### c) Worker (`lambdas/worker/app.py`)
- **Función**: Procesa órdenes para un símbolo específico
- **Características**:
  - Obtiene datos de la API de Bitget
  - Procesa y formatea la respuesta
  - Maneja reintentos y errores

### 3. Cliente Bitget

**Archivo**: `lambdas/bitget_client.py`

Cliente para interactuar con la API de Bitget que incluye:
- Autenticación con API key y firma
- Métodos para obtener datos de órdenes
- Manejo de errores y reintentos

## Flujo de Datos

1. El cliente realiza una petición a la API FastAPI
2. La API valida la entrada y decide si procesar localmente o en AWS
3. **Procesamiento Local**:
   - Usa ThreadPool para procesamiento paralelo
   - Retorna resultados directamente
4. **Procesamiento en AWS**:
   - El coordinador divide la carga de trabajo
   - Los workers procesan en paralelo
   - El collector combina y ordena los resultados
   - Se retorna la respuesta consolidada

## Variables de Entorno

El proyecto utiliza las siguientes variables de entorno:

```
BITGET_API_KEY=tu_api_key
BITGET_SECRET_KEY=tu_secret_key
BITGET_PASSPHRASE=tu_passphrase
AWS_REGION=us-east-1  # Opcional, para despliegue en AWS
```

## Despliegue

### Requisitos
- Python 3.8+
- Docker (para ejecución local con contenedores)
- Cuenta de AWS (para despliegue en la nube)

### Instalación

1. Clonar el repositorio
2. Crear y activar entorno virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: .\venv\Scripts\activate
   ```
3. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

### Ejecución Local

1. Configurar las variables de entorno en un archivo `.env`
2. Iniciar la API:
   ```bash
   python fastapi_app.main
   ```
3. La API estará disponible en `http://127.0.0.1:8000`

### Despliegue en AWS

1. Instalar AWS SAM CLI
2. Construir y desplegar:
   ```bash
   sam build
   sam deploy --guided
   ```

## Uso

### Ejemplo de petición para extraer órdenes:

```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/extract' \
  -H 'Content-Type: application/json' \
  -d '{"symbols": ["BTCUSDT", "ETHUSDT"]}'
```

### Ejemplo de respuesta:

```json
{
  "success": true,
  "data": [...],
  "duration_seconds": 1.23,
  "total_orders": 150
}
```

## Manejo de Errores

El sistema incluye manejo de errores para:
- Tiempos de espera
- Errores de autenticación
- Límites de tasa (rate limits)
- Errores de red

## Contribución

1. Hacer fork del repositorio
2. Crear una rama para tu característica
3. Hacer commit de tus cambios
4. Hacer push a la rama
5. Abrir un Pull Request
