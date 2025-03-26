import os
import sys
import logging
from dotenv import load_dotenv
from fastapi import FastAPI
import boto3


load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Explicitly import and create S3 client



@app.get("/list-objects/")
def list_objects():
    try:
        s3_client = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                 aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                 region_name=os.getenv("AWS_REGION"))
        logger.info("S3 client successfully created")
    except Exception as e:
        logger.error(f"Error creating S3 client: {e}")
        s3_client = None
    if s3_client is None:
        return {"error": "S3 client could not be initialized"}

    bucket_name = os.getenv("AWS_BUCKET_NAME")  # Get bucket name from .env
    region = os.getenv("AWS_REGION", "us-east-1")  # Default region: us-east-1
    logger.info(f"Attempting to list objects in bucket: {bucket_name}")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            files = []
            for obj in response["Contents"]:
                file_name = obj["Key"]
                public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_name}"
                public_url = public_url.replace("+", "%20")  # Replace '+' with '%20' for spaces

                files.append({"file_name": file_name, "public_url": public_url})

            return {"files": files}
        return {"message": "No files found"}
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return {"error": str(e)}
