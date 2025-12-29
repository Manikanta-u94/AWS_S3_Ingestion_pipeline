from datetime import datetime
import json
from io import BytesIO

import boto3
import pandas as pd

# import requests


def lambda_handler(event, context):
    print(event)

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    destination_bucket = event['Records'][0]['s3']['bucket']['name']
    destination_key = "destination/"+source_key.split('/')[-1]

    s3_client = boto3.client(
        "s3",
        # aws_access_key_id=aws_access_key_id,
        # aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1"
    )

    # Read CSV from S3
    response = s3_client.get_object(
        Bucket=source_bucket,
        Key=source_key
    )

    csv_data = response["Body"].read()
    df = pd.read_csv(BytesIO(csv_data))


    # Transform data: Add process data column
    df['process_data'] = datetime.now().strftime('%Y-%m-%d')

    # convert Dataframe to CSV
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Upload back to S3
    s3_client.put_object(
        Bucket=destination_bucket,
        Key=destination_key,
        Body=buffer.getvalue()
    )
    print("CSV file read from S3 and stored to S3 successfully")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }
