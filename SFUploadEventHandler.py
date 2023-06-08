import json
import os
import boto3
import urllib.parse
import base64
from io import BytesIO
from PIL import Image
import requests

def process_document(s3_list, s_string = None):
	region = os.environ['AWS_REGION']
	bucket = os.environ['AWS_BUCKET']
	roleArn = os.environ['ROLE_ARN']
	snsArn = os.environ['SNS_ARN']
	snsArnExpense = os.environ['SNS_ARN_EXPENSE']
	
	expenseTypes = ['Expense_Report__c', 'Expense Report']

	textract = boto3.client('textract', region_name=region)
	s3_client = boto3.client('s3')
	
	
	if len(s3_list) > 0:
		for s3 in s3_list:
			print('s3 >>> ', s3)
			
			# path = s3['File_URL__c']
			path = urllib.parse.unquote(s3['File_URL__c'])
			print('path >>> ', path)
			
			obj_name = path.replace('https://', '')
			bucket_name = obj_name[0 : obj_name.find('.')]
			obj_name = obj_name[obj_name.find('/') + 1 : len(obj_name)]
			
			s3Split = obj_name.split('/')
			s3PathList = s3Split[0 : len(s3Split) - 1]
			s3Path = ''.join([str(item) + '/' for item in s3PathList])
			s3ObjectPath = s3Path + 's3.json'
			
			print('bucket_name >>> ', bucket_name)
			print('obj_name >>> ', obj_name)
			print('s3ObjectPath >>> ', s3ObjectPath)
			
			decoded_session = ''
			url = ''
			
			if s_string != None:
				arr_split = s_string.split('&')
				session = arr_split[0][2:len(arr_split[0])]
				#  Decoding b64 session
				decoded_session = base64.b64decode(session).decode("utf-8")
				url = arr_split[1][2:len(arr_split[1])]
				
				s3['sessionId'] = decoded_session
				s3['instanceUrl'] = url
			
			s3_client.put_object(Body=json.dumps(s3), Bucket=bucket_name, Key=s3ObjectPath)
			
			# Checking if image and if it is a logo, criteria: if image width <450 or height <200
			is_thumbnail = False
			try:
				obj = s3_client.get_object(Bucket=bucket_name, Key=obj_name)['Body'].read()
				img = Image.open(BytesIO(obj))
				# get width and height
				width, height = img.size
				
				if width < 200 or height < 150:
					is_thumbnail = True
					
					delete_record('Document_Manager__c', s3['Related_To_ID__c'], instance_url=url, access_token=decoded_session)
			except Exception as e:
				print('Error getting image dimension >>> ', e)
			
			response = ''
			
			if is_thumbnail:
				print('Thumbnail! Not processing')
			elif ('Object_Api_Name__c' in s3 and s3['Object_Api_Name__c'] in expenseTypes) or ('File_Category__c' in s3 and s3['File_Category__c'] in expenseTypes):
				response = textract.start_document_analysis(
					DocumentLocation={
						'S3Object': {
							'Bucket': bucket_name, 
							'Name': obj_name
						}
					},
					FeatureTypes=["TABLES", "FORMS", "SIGNATURES"],
					NotificationChannel={
						'RoleArn': roleArn, 
						'SNSTopicArn': snsArnExpense
					}
				)
			else:
				response = textract.start_document_analysis(
					DocumentLocation={
						'S3Object': {
							'Bucket': bucket_name, 
							'Name': obj_name
						}
					},
					FeatureTypes=["TABLES", "FORMS", "SIGNATURES"],
					NotificationChannel={
						'RoleArn': roleArn, 
						'SNSTopicArn': snsArn
					}
				)
				
			print('response >>> ', response)
	else:
		print('s3 list empty!')
		
def lambda_handler(event, context):
	#  Event contains the serialized Amazon_S3_Attachment__c record
	print('event >>> ', event)
	
	#  If dict, using session data, else just process normally
	if isinstance(event, dict):
		s_string = event['sString']
		s3_json = event['s3JSON']
		s3_list = json.loads(s3_json)
		process_document(s3_list, s_string)
	elif isinstance(event, list):
		process_document(event)
	
	return {
		'statusCode': 200,
		'body': json.dumps('Textract process started!')
	}
	

def sf_api_call(action, parameters = {}, method = 'get', data = {}, instance_url = None, access_token = None):
	"""
	Helper function to make calls to Salesforce REST API.
	Parameters: action (the URL), URL params, method (get, post or patch), data for POST/PATCH.
	"""
	# if instance_url == None or access_token == None:
	# 	instance_url, access_token = sf_authenticate()

	headers = {
		'Content-type': 'application/json',
		'Accept-Encoding': 'gzip',
		'Authorization': 'Bearer %s' % access_token
	}

	if method in ['get', 'delete']:
		r = requests.request(method, instance_url+action, headers=headers, params=parameters, timeout=3000)
	elif method in ['post', 'patch']:
		r = requests.request(method, instance_url+action, headers=headers, json=data, params=parameters, timeout=1000)
	else:
		# other methods not implemented in this example
		raise ValueError('Method should be get or post or patch.')

	print('Debug: API %s call: %s' % (method, r.url) )
	if r.status_code < 300:
		if method in ['patch', 'delete']:
			return None
		else:
			return r.json()
	else:
		raise Exception('API error when calling %s : %s' % (r.url, r.content))

		
def delete_record(sobject_name, record_id, instance_url = None, access_token = None):
	print('deleting >>> ', record_id)
	action = '/services/data/v54.0/sobjects/'

	if sobject_name != None and record_id != None:
		return sf_api_call(action + sobject_name + '/' + record_id, method='delete', instance_url=instance_url, access_token=access_token)
	else:
		return None
