from urllib import response

import requests
import boto3
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import date

def sync_bls_files_to_s3(bucket, s3_key, url, regex):

    s3_client = boto3.client('s3')
    
    today = date.today()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    # 1. Get webpage and find files
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a['href'] for a in soup.find_all('a', href=True)]

    matched_files = [link for link in links] 
   
    # 3. Download and Upload
    for file_url in matched_files:
        full_url = urljoin(url, file_url)
        file_name = file_url.split('/')[-1]
    
        # Download
        file_data = requests.get(full_url).content
        key = f'{s3_key}/{today}/{file_name}'
        
        # Upload to S3
        s3_client.put_object(Bucket=bucket, Key=key, Body=file_data)
        print(f"Uploaded {file_name} to {bucket} with key {key}...")


bucket = 'rearc-demo-dk'
s3_key= 'inbound'
url = 'https://download.bls.gov/pub/time.series/pr/'
regex = r'*'  

sync_bls_files_to_s3(bucket, s3_key, url, regex)