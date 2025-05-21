import boto3
import json
import os

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    """
    AWS Lambda function to trigger an AWS Step Function execution upon the
    arrival of specific S3 files.

    This function checks if all required files are present in the specified S3
    bucket and triggers a Step Function execution if no execution is currently
    running. It uses the environment variable 'STATE_MACHINE_ARN' to determine
    the ARN of the Step Function to execute.

    Parameters:
        event (dict): The S3 event data containing bucket and object information.
        context (object): The Lambda context object.

    Returns:
        dict: A dictionary with the status of the operation, including any
              execution ARN if a Step Function was triggered.

    Possible return statuses:
        - "Waiting for required files": Not all required files are available.
        - "Already running": A Step Function execution is already in progress.
        - "Step Function triggered": A new Step Function execution was started.
        - "Error checking Step Function": Error occurred while checking for
          running executions.
        - "Error triggering Step Function": Error occurred while starting a
          new execution.
    """

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    state_machine_arn = os.environ['STATE_MACHINE_ARN']

    # Required files
    required_keys = [
        'raw-data/products.csv',
        'raw-data/orders.csv',
        'raw-data/order_items.csv'
    ]

    # Check if all required files are present
    all_present = True
    for key in required_keys:
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
        except s3.exceptions.ClientError:
            all_present = False
            break
    
    # Return if not all required files are present
    if not all_present:
        print("Not all required files are present yet.")
        return {
            "status": "Waiting for required files"
        }

    # Check if any execution is currently running
    try:
        executions = stepfunctions.list_executions(
            stateMachineArn=state_machine_arn,
            statusFilter='RUNNING'
        )

        # Return if an execution is already running
        if executions['executions']:
            print("A Step Function execution is already in progress. Skipping.")
            return {
                "status": "Already running",
                "executionArn": executions['executions'][0]['executionArn']
            }
    except Exception as e:
        print("Error checking Step Function executions:", str(e))
        return {
            "status": "Error checking Step Function",
            "error": str(e)
        }

    # Start Step Function
    try:
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
