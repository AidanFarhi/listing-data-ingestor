import os
import boto3
import requests
import json
import snowflake.connector as snowflake_connector
from time import sleep
from dotenv import load_dotenv
from datetime import date
load_dotenv()


def get_list_of_zip_codes(conn):
	cursor = conn.cursor()
	cursor.execute("SELECT zip_code FROM dim_location WHERE state = 'DE'")
	results = (r[0] for r in cursor.fetchall())
	conn.close()
	return results

def get_results_for_zipcodes(zipcodes, api_key):
	url = 'https://realty-mole-property-api.p.rapidapi.com/saleListings'
	querystring = {'state':'DE', 'limit': '500', 'propertyType': 'Single Family', 'status': 'Active'}
	headers = {
		'X-RapidAPI-Key': api_key,
		'X-RapidAPI-Host': 'realty-mole-property-api.p.rapidapi.com'
	}
	results = []
	for zip in list(zipcodes)[:3]:
		sleep(.6)
		querystring['zipCode'] = zip
		response = requests.get(url=url, headers=headers, params=querystring)
		results.append(response.json())
	return results

def load_results_to_s3(client, results, bucket_name):
	for i, obj in enumerate(results):
		if len(obj) > 0:
			json_data = json.dumps(obj)
			client.put_object(
				Bucket=bucket_name, 
				Key=f'real_estate/listings/{date.today()}/result_{i}.json',
				Body=json_data
			)

def main(event, context):
	bucket_name = os.getenv('BUCKET_NAME')
	api_key = os.getenv('RAPID_API_KEY')
	client = boto3.client(
		's3', 
		endpoint_url='https://s3.amazonaws.com',
		aws_access_key_id=os.getenv('ACCESS_KEY'),
		aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
	)
	conn = snowflake_connector.connect(
		user=os.getenv('SNOWFLAKE_USERNAME'),
		password=os.getenv('SNOWFLAKE_PASSWORD'),
		account=os.getenv('SNOWFLAKE_ACCOUNT'),
		warehouse=os.getenv('WAREHOUSE'),
		database=os.getenv('DATABASE'),
		schema=os.getenv('SCHEMA')
	)
	zipcodes = get_list_of_zip_codes(conn)
	results = get_results_for_zipcodes(zipcodes, api_key)
	load_results_to_s3(client, results, bucket_name)
	return {'statusCode': 200}
