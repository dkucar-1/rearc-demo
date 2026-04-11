#!/usr/bin/env python3
from urllib import response

import requests
import boto3
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import date
from io import BytesIO

def sync_file_to_s3(url, bucket, s3_key, output_file=None):
    
    today = date.today()

    # if output_file is not provided, it is implied from the last part of the url
    output_file = url.split('/')[-1] if not output_file else output_file

    # provide contact info in headers to avoid errors from some servers that block unknown agents. This is a good practice when scraping or downloading data.
    headers = {
        'User-Agent': 'RearcDemo/1.0 (contact: intrepid_cool_lady@yahoo.com)'
    }

    try:
    # 2. Download the data
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Check for 403 or other errors

    # 3. Upload directly to S3
        s3 = boto3.client('s3')
        key = f'{s3_key}/{today}/{output_file}'
        
        s3.upload_fileobj(BytesIO(response.content), bucket, key)
        print(f"Successfully uploaded to s3://{bucket}/{key}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")

if __name__ =="__main__": 

    bucket = 'rearc-demo-dk'
    s3_key = 'inbound'
    url = 'https://download.bls.gov/pub/time.series/pr/pr.data.0.Current'  
    sync_file_to_s3(url, bucket, s3_key)  

    url = "https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&drilldowns=State,Year&measures=Population"
    sync_file_to_s3(url, bucket, s3_key, output_file='datausa_population.json')