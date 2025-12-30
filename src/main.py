import json

from src.services.s3_to_s3_service import S3ToS3

def lambda_handler(event, _):
    print(event)
    service = S3ToS3(event)

    service.read()
    service.transform()
    service.load()

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
                # "location": ip.text.replace("\n", "")
            }
        ),
    }
