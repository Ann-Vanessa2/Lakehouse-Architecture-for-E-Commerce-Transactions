import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    This AWS Lambda function archives files from a specified S3 bucket prefix
    to another prefix.

    Parameters:
        event (dict): The event data from the S3 invocation, containing bucket and
                      object information.
        context (object): The Lambda context object.

    Returns:
        dict: A dictionary with a message indicating if the archiving was successful.

    Example event:
        {
            'sourceBucket': 'my-bucket',
            'sourcePrefix': 'raw-data/',
            'archivePrefix': 'archive/'
        }
    """

    # Get event parameters
    source_bucket = event['sourceBucket']
    source_prefix = event['sourcePrefix']
    archive_prefix = event['archivePrefix']

    # Get current timestamp
    now = datetime.utcnow()
    timestamp_path = now.strftime('%Y-%m-%d_%H-%M-%S')

    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    # Check if any files were found
    if 'Contents' not in response:
        return {'message': 'No files found to archive.'}

    # Copy and delete files
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('/'):  # Skip folders
            continue

        # Copy file
        copy_source = {'Bucket': source_bucket, 'Key': key}

        # destination_key = key.replace(source_prefix, archive_prefix, 1)

        # Strip off the prefix and prepend the timestamped archive path
        filename = key.replace(source_prefix, '', 1)
        destination_key = f"{archive_prefix}{timestamp_path}/{filename}"


        # copying the files
        print(f"Copying {key} to {destination_key}")
        s3.copy_object(Bucket=source_bucket, CopySource=copy_source, Key=destination_key)
        s3.delete_object(Bucket=source_bucket, Key=key)

    return {'message': 'Files archived successfully to {archive_prefix}{timestamp_path}/'}
