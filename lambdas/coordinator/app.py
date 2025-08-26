# Handler: recibe symbols y StartExecution

import json
import boto3
import time

def lambda_handler(event, context):
    """Coordinator Lambda: Start Step Functions execution"""
    
    symbols = event.get('symbols', [])
    
    if not symbols:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No symbols provided'})
        }
    
    stepfunctions = boto3.client('stepfunctions')
    
    input_data = {
        'symbols': symbols,
        'startTime': time.time()
    }
    
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=context.invoked_function_arn.replace(':function:', ':states:') + '-StateMachine',
            input=json.dumps(input_data)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'executionArn': response['executionArn'],
                'message': 'Extraction started successfully'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }