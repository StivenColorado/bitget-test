pip install python-dotenv
pip install fastapi
pip install uvicorn
pip install requests
pip install boto3
pip install pydantic
pip install localstack

#SAM
create a venv with python 3.11
install SAM CLI
#command
sam build

#set fake credentials
# Configura las credenciales temporales
$env:AWS_ACCESS_KEY_ID="test"
$env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"

#listar buckets locales
aws --endpoint-url=http://localhost:4566 s3 ls
#crear bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://bitget-results

sam validate --region us-east-1