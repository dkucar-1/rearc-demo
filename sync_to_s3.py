from urllib import response

import requests
import boto3
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import date

def sync_bls_files_to_s3(url, bucket, s3_key):

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
        data = requests.get(full_url).content
        key = f'{s3_key}/{today}/{file_name}'
        
        # Upload to S3
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        print(f"Uploaded {file_name} to {bucket} with key {key}...")


def sync_datausa_to_s3(url, bucket, key):
    import requests

    s3_client = boto3.client('s3')

    # 2. Send the GET request
    response = requests.get(url)
    key = f'{key}/datausa_population.json'

    # 3. Check if the request was successful (status code 200)
    if response.status_code == 200:
        # 4. Parse the JSON response
        data = response.content
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    else:
        print(f"Error: {response.status_code}")


bucket = 'rearc-demo-dk'
s3_key= 'inbound'
url = 'https://download.bls.gov/pub/time.series/pr/'

#sync_bls_files_to_s3(url, bucket, s3_key)

url = "https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&drilldowns=State,Year&measures=Population"

sync_datausa_to_s3(url, bucket, s3_key)