import boto3
import json
import os

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    """
    AWS Lambda function to trigger an AWS Step Function based on S3 file events.

    This function is invoked by an S3 event when files are uploaded. It checks for
    the existence of specific files in an S3 bucket and triggers a Step Function 
    if all required conditions are met. It ensures that the Step Function is only 
    triggered once by checking for the existence of a status file.

    Args:
        event (dict): The event data from the S3 invocation, containing bucket and 
                      object information.
        context (object): The Lambda context object.

    Returns:
        dict: A dictionary with the status of the operation, including whether 
              the Step Function execution has been triggered or if the function
              is waiting for required files.
    """

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    state_machine_arn = os.environ['STATE_MACHINE_ARN']  # Get the Step Function ARN from environment variables

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
    
    # Checking for products.csv
    try:
        s3.head_object(Bucket=bucket_name, Key='raw-data/products.csv')
        has_products = True
    except s3.exceptions.ClientError:
        has_products = False

    # Checking for at least one file in orders/
    orders = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw-data/orders/')
    has_orders = 'Contents' in orders and any(
        not obj['Key'].endswith('/') for obj in orders['Contents']
    )

    # Checking for at least one file in order_items/
    order_items = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw-data/order_items/')
    has_order_items = 'Contents' in order_items and any(
        not obj['Key'].endswith('/') for obj in order_items['Contents']
    )


    # Trigger the Step Function if all required files are present
    if has_products and has_orders and has_order_items:
        try:
            # Create a status file to indicate the Step Function was triggered
            s3.put_object(Bucket=bucket_name, Key=status_key, Body='Step Function Triggered')

            # Start the Step Function
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

    # If not all required files are present, return waiting status
    return {
        "status": "Waiting for all required files",
        "has_products": has_products,
        "has_orders": has_orders,
        "has_order_items": has_order_items
    }

