import logging
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd

from src.abstracts.etl_abstract import ETlAbstract
from src.commons import config
from src.utils.etl_logs import logger


class S3ToS3(ETlAbstract):
    """
    ETL pipeline that reads a CSV file from an S3 source bucket,
    applies transformations, and writes the processed file to
    a destination S3 bucket.

    The class is designed to be executed via an AWS Lambda function
    triggered by an S3 ObjectCreated event.
    """

    def __init__(self, event):
        """
        Initialize the ETL pipeline and extract event metadata.

        Parameters
        ----------
        event : dict
            AWS S3 event payload containing bucket and object details.
        """
        logger.info("Initializing S3ToS3 ETL process")

        self.s3_client = boto3.client("s3", region_name=config.region_name)

        self.source_bucket = config.source_bucket
        self.source_key = event["Records"][0]["s3"]["object"]["key"]
        self.file_name = self.source_key.split("/")[-1]

        self.destination_bucket = config.destination_bucket
        self.destination_key = config.destination_key

        self.df = None

        logger.info(
            "Event received | source_bucket=%s | key=%s | file=%s",
            self.source_bucket,
            self.source_key,
            self.file_name,
        )

    def read(self):
        logger.info("Reading file from S3")

        if config.source_key not in self.source_key:
            logger.error(
                "Invalid source folder | expected_prefix=%s | key=%s",
                config.source_key,
                self.source_key,
            )
            raise ValueError("Invalid source folder")

        response = self.s3_client.get_object(
            Bucket=self.source_bucket, Key=self.source_key
        )

        csv_data = response["Body"].read()
        self.df = pd.read_csv(BytesIO(csv_data))

        logger.info(
            "File read successfully | rows=%s | columns=%s",
            self.df.shape[0],
            self.df.shape[1],
        )

    def transform(self):
        logger.info("Transforming dataframe")

        self.df["process_data"] = datetime.now().strftime("%Y-%m-%d")

        logger.info("Transformation complete | columns=%s", list(self.df.columns))

    def load(self):
        logger.info(
            "Uploading file to destination | bucket=%s | key=%s%s",
            self.destination_bucket,
            self.destination_key,
            self.file_name,
        )

        buffer = BytesIO()
        self.df.to_csv(buffer, index=False)
        buffer.seek(0)

        self.s3_client.put_object(
            Bucket=self.destination_bucket,
            Key=self.destination_key + self.file_name,
            Body=buffer.getvalue(),
        )

        logger.info(
            "Upload complete | destination=%s/%s%s",
            self.destination_bucket,
            self.destination_key,
            self.file_name,
        )
