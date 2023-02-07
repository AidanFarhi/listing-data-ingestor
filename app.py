import os
import boto3
import requests
import json
from dotenv import load_dotenv
from datetime import date
from time import sleep
load_dotenv()

def paginate_api(url, headers, querystring):
    items = []
    offset = 0
    limit = 200
    while True:
        querystring['offset'] = offset
        querystring['limit'] = limit
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        items.extend(data['listings'])
        if offset + limit >= data['totalResultCount']:
            break
        offset += limit
        sleep(.1)
    return items

def load_to_s3(client, data):
	json_data = json.dumps(data)
	client.put_object(
		Bucket=os.getenv('BUCKET_NAME'), 
		Key=f'real_estate/listings/{date.today()}.json',
		Body=json_data
	)
	client.put_object(
	    Bucket=os.getenv('BUCKET_NAME'), 
	    Key=f'real_estate/listings/latest.json',
	    Body=json_data
	)

def main():
	url = 'https://us-real-estate-listings.p.rapidapi.com/for-sale'
	querystring = {'location': 'Delaware', 'property_type': 'single_family'}
	headers = {
		'X-RapidAPI-Key': os.getenv('RAPID_API_KEY'),
		'X-RapidAPI-Host': 'us-real-estate-listings.p.rapidapi.com'
	}
	client = boto3.client(
	    's3', 
	    endpoint_url=os.getenv('ENDPOINT_URL'),
	    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
	    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
	)
	data = paginate_api(url, headers, querystring)
	load_to_s3(client, data)

if __name__ == '__main__':
	main()
