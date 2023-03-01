import os
import boto3
import requests
import json
import snowflake.connector as snowflake_connector
from time import sleep
from dotenv import load_dotenv
from datetime import date
load_dotenv()


# def load_to_s3(client, data):
# 	client = boto3.client(
# 	    's3', 
# 	    endpoint_url=os.getenv('ENDPOINT_URL'),
# 	    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
# 	    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
# 	)
# 	json_data = json.dumps(data)
# 	client.put_object(
# 		Bucket=os.getenv('BUCKET_NAME'), 
# 		Key=f'real_estate/listings/{date.today()}.json',
# 		Body=json_data
# 	)
# 	client.put_object(
# 	    Bucket=os.getenv('BUCKET_NAME'), 
# 	    Key=f'real_estate/listings/latest.json',
# 	    Body=json_data
# 	)


def get_list_of_zip_codes():
	conn_params = {
		'user': os.getenv('SNOWFLAKE_USERNAME'),
		'password': os.getenv('SNOWFLAKE_PASSWORD'),
		'account': os.getenv('SNOWFLAKE_ACCOUNT'),
		'warehouse': 'COMPUTE_WH',
		'database': 'DEV_DATABASE',
		'schema': 'PUBLIC',
	}
	conn = snowflake_connector.connect(**conn_params)
	cursor = conn.cursor()
	cursor.execute("SELECT zip FROM ZIPCODE WHERE state = 'DE'")
	results = (r[0] for r in cursor.fetchall())
	conn.close()
	return results

def get_results_for_zipcodes(zipcodes):
	url = 'https://realty-mole-property-api.p.rapidapi.com/saleListings'
	querystring = {'state':'DE', 'limit': '500', 'propertyType': 'Single Family', 'status': 'Active'}
	headers = {
		'X-RapidAPI-Key': os.getenv('RAPID_API_KEY'),
		'X-RapidAPI-Host': 'realty-mole-property-api.p.rapidapi.com'
	}
	count = 0
	for zip in zipcodes:
		sleep(.6)
		querystring['zipCode'] = zip
		response = requests.get(url=url, headers=headers, params=querystring)
		data = response.json()
		with open(f'raw_data/result_{count}.json', 'w') as f:
			json.dump(data, f)
		count += 1

def main():
	zipcodes = get_list_of_zip_codes()
	get_results_for_zipcodes(zipcodes)


if __name__ == '__main__':
	main()
