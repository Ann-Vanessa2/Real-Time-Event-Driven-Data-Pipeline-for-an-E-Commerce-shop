import boto3
import json
import os

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    state_machine_arn = os.environ['STATE_MACHINE_ARN']

    # Check if a status file exists
    status_key = 'status/execution_started.txt'
    try:
        s3.head_object(Bucket=bucket_name, Key=status_key)
        print("Step Function already triggered. Skipping execution.")
        return {
            "status": "Already triggered",
            "message": "Step Function execution has already been triggered."
        }
    except s3.exceptions.ClientError:
        # Status file does not exist, proceed with checking files
        pass
    
    # Check for products.csv
    try:
        s3.head_object(Bucket=bucket_name, Key='raw-data/products.csv')
        has_products = True
    except s3.exceptions.ClientError:
        has_products = False

    # Check for at least one file in orders/
    orders = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw-data/orders/')
    has_orders = 'Contents' in orders and any(
        not obj['Key'].endswith('/') for obj in orders['Contents']
    )

    # Check for at least one file in order_items/
    order_items = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw-data/order_items/')
    has_order_items = 'Contents' in order_items and any(
        not obj['Key'].endswith('/') for obj in order_items['Contents']
    )

    if has_products and has_orders and has_order_items:
        try:
            # Create a status file to indicate the Step Function was triggered
            s3.put_object(Bucket=bucket_name, Key=status_key, Body='Step Function Triggered')

            response = stepfunctions.start_execution(
                stateMachineArn=state_machine_arn,
                input=json.dumps({"triggered_by": "S3 Lambda Trigger"})
            )
            print("Step Function execution started:", response['executionArn'])
            return {
                "status": "Step Function triggered",
                "executionArn": response['executionArn']
            }
        except Exception as e:
            print("Failed to start Step Function:", str(e))
            return {
                "status": "Error triggering Step Function",
                "error": str(e)
            }

    return {
        "status": "Waiting for all required files",
        "has_products": has_products,
        "has_orders": has_orders,
        "has_order_items": has_order_items
    }

