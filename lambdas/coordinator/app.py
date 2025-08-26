def lambda_handler(event, context):
    """Coordinator Lambda: Start Step Functions execution"""

    import boto3, os, json, time

    symbols = event.get('symbols', [])
    if not symbols:
        return {'statusCode': 400, 'body': json.dumps({'error': 'No symbols provided'})}

    stepfunctions = boto3.client(
        'stepfunctions',
        region_name=os.getenv('AWS_REGION', 'us-east-1'),
        aws_access_key_id='test',
        aws_secret_access_key='test',
        endpoint_url=os.getenv('STEP_FUNCTIONS_ENDPOINT', 'http://localhost:4566')
    )

    input_data = {'symbols': symbols, 'startTime': time.time()}

    try:
        response = stepfunctions.start_execution(
            stateMachineArn=os.getenv('STATE_MACHINE_ARN'),
            input=json.dumps(input_data)
        )
        return {'statusCode': 200, 'body': json.dumps({'executionArn': response['executionArn'], 'message': 'Extraction started successfully'})}

    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
