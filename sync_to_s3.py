#!/usr/bin/env python3
import requests
import boto3
from io import BytesIO
import hashlib
from botocore.exceptions import ClientError

def sync_file_to_s3(url, bucket, s3_key, output_file=None):
    # if output_file is not provided, it is implied from the last part of the url
    output_file = url.split('/')[-1] if not output_file else output_file

    # provide contact info in headers to avoid errors from some servers that block unknown agents. This is a good practice when scraping or downloading data.
    headers = {
        'User-Agent': 'RearcDemo/1.0 (contact: intrepid_cool_lady@yahoo.com)'
    }

    s3_client = boto3.client('s3')

    response = requests.get(url, headers=headers, timeout=30)
    new_md5 = hashlib.md5(response.content).hexdigest()

    # check what's currently in S3 (if anything)
    try:
        head = s3_client.head_object(Bucket=bucket, Key=f"{s3_key}/{output_file}")
        current_etag = head["ETag"].strip('"')
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            current_etag = None
            print(f"No existing {output_file} file. Will attempt to create.")
        else:
            raise

    # content doesn't exist; conditional create
    if current_etag is None:
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=f"{s3_key}/{output_file}",
                Body=BytesIO(response.content),
                IfNoneMatch="*",
            )
            print("Files written successfully.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "PreconditionFailed":
                print("Race condition detected. Someone else is attempting to write the file.")
            raise

    # content exists and content is identical; no update needed
    elif current_etag == new_md5:
        print(f"File is present and content is identical. No update needed.")

    # content exists and content differs; conditional overwrite
    else:
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=f"{s3_key}/{output_file}",
                Body=BytesIO(response.content),
                IfMatch=current_etag,
            )
            print(f"File is present but content differs. Uploading to {output_file} with new content.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "PreconditionFailed":
                print(f"File is present and content differs. Someone else updated it first.")
            raise

if __name__ =="__main__": 

    bucket = 'demo-dk'
    s3_key = 'inbound'
    url = 'https://download.bls.gov/pub/time.series/pr/pr.data.0.Current'  
    sync_file_to_s3(url, bucket, s3_key)  

    url = "https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&drilldowns=State,Year&measures=Population"
    sync_file_to_s3(url, bucket, s3_key, output_file='datausa_population.json')