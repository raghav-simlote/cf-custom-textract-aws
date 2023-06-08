import os
import requests
import json
import re
import boto3
import PyPDF2
from io import BytesIO
from datetime import datetime


def sf_authenticate():
	url = ''
	params = {}
	
	SF_CLIENT_ID = ''
	SF_CLIENT_SECRET = ''
	SF_PASSWORD = ''
	SF_USERNAME = ''
	SF_URL = ''
	
	if prod:
		print('sf authenticate using production')
		SF_CLIENT_ID = os.environ['SF_CLIENT_ID']
		SF_CLIENT_SECRET = os.environ['SF_CLIENT_SECRET']
		SF_PASSWORD = os.environ['SF_PASSWORD']
		SF_USERNAME = os.environ['SF_USERNAME']
		SF_URL = os.environ['SF_URL']
	elif staging:
		SF_CLIENT_ID = os.environ['SF_CLIENT_ID_STAGING']
		SF_CLIENT_SECRET = os.environ['SF_CLIENT_SECRET_STAGING']
		SF_PASSWORD = os.environ['SF_PASSWORD_STAGING']
		SF_USERNAME = os.environ['SF_USERNAME_STAGING']
		SF_URL = os.environ['SF_URL_SANDBOX']
	elif dev:
		SF_CLIENT_ID = os.environ['SF_CLIENT_ID_DEV']
		SF_CLIENT_SECRET = os.environ['SF_CLIENT_SECRET_DEV']
		SF_PASSWORD = os.environ['SF_PASSWORD_DEV']
		SF_USERNAME = os.environ['SF_USERNAME_DEV']
		SF_URL = os.environ['SF_URL_SANDBOX']
	
	url = SF_URL
	params = {
		"grant_type": "password",
		"client_id": SF_CLIENT_ID, # Consumer Key
		"client_secret": SF_CLIENT_SECRET, # Consumer Secret
		"username": SF_USERNAME, # The email you use to login
		"password": SF_PASSWORD # Concat your password and your security token
	}

	r = requests.post(url, params=params)

	print("status code >>>> " + str(r.status_code))
	print(str(r.json()))

	# if you connect to a Sandbox, use test.salesforce.com instead
	access_token = r.json().get("access_token")
	instance_url = r.json().get("instance_url")

	print("Access Token:", access_token)
	print("Instance URL", instance_url)

	return instance_url, access_token

def sf_api_call(action, parameters = {}, method = 'get', data = {}, instance_url = None, access_token = None):
	"""
	Helper function to make calls to Salesforce REST API.
	Parameters: action (the URL), URL params, method (get, post or patch), data for POST/PATCH.
	"""
	if instance_url == None or access_token == None:
		instance_url, access_token = sf_authenticate()

	headers = {
		'Content-type': 'application/json',
		'Accept-Encoding': 'gzip',
		'Authorization': 'Bearer %s' % access_token
	}

	print('method >>> ', method)

	if method == 'get':
		r = requests.request(method, instance_url+action, headers=headers, params=parameters, timeout=30)
	elif method in ['post', 'patch']:
		r = requests.request(method, instance_url+action, headers=headers, json=data, params=parameters, timeout=10)
	else:
		# other methods not implemented in this example
		raise ValueError('Method should be get or post or patch.')

	print('Debug: API %s call: %s' % (method, r.url) )
	if r.status_code < 300:
		if method=='patch':
			return None
		else:
			return r.json()
	else:
		raise Exception('API error when calling %s : %s' % (r.url, r.content))
		
def update_record(sobject_name, record_id, data, instance_url = None, access_token = None):
	print('updating >>> ', record_id)
	print('data >>> ', data)
	action = '/services/data/v54.0/sobjects/'

	if sobject_name != None and record_id != None:
		return sf_api_call(action + sobject_name + '/' + record_id, method='patch', data=data, instance_url=instance_url, access_token=access_token)
	else:
		return None

def create_record(sobject_name, data, instance_url = None, access_token = None):
	print('creating >>> ', sobject_name)
	print('data >>> ', data)
	action = '/services/data/v54.0/sobjects/'

	return sf_api_call(action + sobject_name + '/', method='post', data=data, instance_url=instance_url, access_token=access_token)

def start(textMessage):
	s3_client = boto3.client('s3')
	
	jobId = textMessage['jobId']
	s3Bucket = textMessage['s3Bucket']
	s3ObjectName = textMessage['s3ObjectName']
	s3Path = textMessage['s3Path']
	pKey = textMessage['pKey']
	splitReference = textMessage['splitReference']
	pageType = textMessage['pageType']
	confidenceLevel = textMessage['confidenceLevel'] if 'confidenceLevel' in textMessage else 0
	pages = textMessage['pages'] if 'pages' in textMessage else 0
	
	global prod
	global staging
	global dev

	prod = False
	staging = False
	dev = False
	
	if s3Bucket == 'horsepowernyc':
		prod = True
		staging = False
		dev = False
	elif s3Bucket == 'horsepower-uat':
		prod = False
		staging = True
		dev = False
	else:
		prod = False
		staging = False
		dev = True
	
	# Path to get the page results
	textractPageResultPath = s3Path + 'pages/'
	s3ObjectPath = s3Path + 's3.json'
	
	s3Split = s3ObjectName.split('/')
	baseURL = s3Split[0] + '/'
	parentId = s3Split[1]
	objectAPIName = s3Split[0]
	
	print('jobId >>> ', jobId)
	print('s3Bucket >>> ', s3Bucket)
	print('s3ObjectName >>> ', s3ObjectName)
	print('s3Path >>> ', s3Path)
	print('pKey >>> ', pKey)
	print('splitReference >>> ', splitReference)
	print('pageType >>> ', pageType)
	print('objectAPIName >>> ', objectAPIName)
	print('pages >>> ', pages)
	
	# regex for filenames
	pattern = re.compile('[ #+~%:*?<>\{\}]')
	
	# output folder name
	now = datetime.now() # current date and time
	unique_filename = now.strftime("%Y-%m-%d-%H%M%S")
	
	# Regex-ed Page Name
	fileName = pattern.sub("_", pKey)
	
	# Getting source file
	pdf_obj = s3_client.get_object(Bucket=s3Bucket, Key=s3ObjectName)
	fs = pdf_obj['Body'].read()
					
	# Writing split PDF to S3
	pdfWriter = PyPDF2.PdfFileWriter()
	
	textractBlocks = []
	
	if splitReference != None:
		pdfFile = PyPDF2.PdfFileReader(BytesIO(fs), strict=False)
		
		for split in splitReference:
			print('split >>> ' + str(split))
		
			# Compiling Textract results for 
			json_obj = s3_client.get_object(Bucket=s3Bucket, Key=textractPageResultPath + str(split) + '.json')
			j_results = json.loads(json_obj['Body'].read().decode('utf-8'))
			textractBlocks.extend(j_results['Blocks'])
			
			# Generating PDF based on split
			pdfWriter.addPage(pdfFile.getPage(split - 1))
			
		print('final count >>> ' + str(len(textractBlocks)))
	else:
		try:
			json_obj = s3_client.get_object(Bucket=s3Bucket, Key=textractPageResultPath + str(1) + '.json')
			j_results = json.loads(json_obj['Body'].read().decode('utf-8'))
			# textractBlocks.extend(j_results['Blocks'])
			textractBlocks = j_results['Blocks']
		except:
			print('couldnt find the blocks')
	
	#  Getting Textract Result data from S3
	forms_obj = s3_client.get_object(Bucket=s3Bucket, Key=textractPageResultPath + pKey + '_forms.json')
	forms_results = forms_obj['Body'].read().decode('utf-8')
	
	tables_obj = s3_client.get_object(Bucket=s3Bucket, Key=textractPageResultPath + pKey + '_tables.json')
	tables_results = tables_obj['Body'].read().decode('utf-8')
	
	#  Getting Amazon S3 Attachment record information
	s3_obj = s3_client.get_object(Bucket=s3Bucket, Key=s3ObjectPath)
	s3_data = json.loads(s3_obj['Body'].read().decode('utf-8'))
	
	
	s3FileNameWithExtension = s3_data['File_Name__c']
	s3FileName = s3Split[-1].split('.')[0]
	s3FileNameExtension = '.' + s3Split[-1].split('.')[-1]
	#s3FileType = s3_data['File_Type__c']
	s3Id = s3_data['Id']
	
	print('s3_data >>> ', s3_data)
	
	#  If session Id passed through, use sessionId
	access_token = None
	instance_url = None
	if 'sessionId' in s3_data.keys():
		access_token = s3_data['sessionId']
	if 'instanceUrl' in s3_data.keys():
		instance_url = s3_data['instanceUrl']
			
	#  Fix for unidentified
	docCategory = pageType
	docType = 'Processed Document'
	
	docManager_id = None

	#  If just one page, use Source Document
	if pages == 1 and objectAPIName == 'Document_Manager__c':
		response_docManager = update_record('Document_Manager__c', parentId, {
			"Document_Type__c" : docType,
			"Document_Category__c" : docCategory,
			"Status__c" : "In Process",
			"Analysis_Status__c" : "Analysis in Progress",
			"Subject__c" : pKey,
			#"Record_Id__c" : s3_data["Record_Id__c"],
			"Record_Id__c" : s3_data["Related_To_ID__c"],
			"Uploaded_Amazon_S3_Attachment__c" : s3Id,
			"Confidence_Level__c" : confidenceLevel
		}, instance_url=instance_url, access_token=access_token)
		
		docManager_id = parentId
	else:
		if objectAPIName != 'Document_Manager__c':
			parentId = None
			
		#  Creating the Document Manager record in SF
		response_docManager = create_record('Document_Manager__c', {
			"Parent_Document_Manager__c" : parentId,
			"Document_Type__c" : docType,
			"Document_Category__c" : docCategory,
			"Status__c" : "In Process",
			"Analysis_Status__c" : "Analysis in Progress",
			"Subject__c" : pKey,
			#"Record_Id__c" : s3_data["Record_Id__c"],
			"Record_Id__c" : s3_data["Related_To_ID__c"],
			"Uploaded_Amazon_S3_Attachment__c" : s3Id,
			"Confidence_Level__c" : confidenceLevel
		}, instance_url=instance_url, access_token=access_token)
	
		if response_docManager != None and response_docManager["success"] == True:
			docManager_id = response_docManager["id"]
			
	if docManager_id != None:
		print('docmanager id >>> ' + docManager_id)
					
		response = create_record('Amazon_S3_Attachment__c', {
			"Related_To_ID__c" : docManager_id,
			"File_Name__c" : fileName + '.pdf',
			"File_Type__c" : "Uploaded/Sent",
			"Object_Api_Name__c" : "Document_Manager__c",
			"isMergable__c" : False,
			"Display_Order__c" : 1,
			"Tables_Data__c" : tables_results,
			"Forms_Data__c" : forms_results
		}, instance_url=instance_url, access_token=access_token)
		
		if response["success"] == True:
			s3_id = response["id"]
			
			filePath = baseURL + docManager_id + '/' + s3_id + '/' + fileName + s3FileNameExtension
			filePathJson = baseURL + docManager_id + '/' + s3_id + '/' + fileName + '.json'
			outputpdf = '/tmp/' + fileName + '.pdf'
			outputjson = '/tmp/' + fileName + '.json'
			
			s3_resource = boto3.resource('s3')
					
			if splitReference != None:
				# writing split pdf pages to pdf file
				with open(outputpdf, "wb") as f:
					pdfWriter.write(f)
					
				s3_resource.Bucket(s3Bucket).upload_file(outputpdf, filePath, ExtraArgs={"ContentType": "application/pdf"})
			# if no split; just move over original file
			else:
				s3_resource.Object(s3Bucket, filePath).copy_from(CopySource=s3Bucket + '/' + s3ObjectName)
				
			# writing split json response to json file
			with open(outputjson, "w") as f:
				f.write(json.dumps({'Blocks' : textractBlocks}))
				
			s3_resource.Bucket(s3Bucket).upload_file(outputjson, filePathJson, ExtraArgs={"ContentType": "application/json"})
			
			fileURL = 'https://' + s3Bucket + '.s3.amazonaws.com/' + filePath
			
			# Updating S3 Attachment record for File Path URL
			update_record('Amazon_S3_Attachment__c', s3_id, {
				"File_URL__c" : fileURL
			}, instance_url=instance_url, access_token=access_token)
			
			# Updating Analysis Status of Document Manager record to trigger function on trigger
			update_record('Document_Manager__c', docManager_id, {
				"Analysis_Status__c" : "Human Loop"
			}, instance_url=instance_url, access_token=access_token)

def process_message(message):
	print('message >>> ', message)
	print('messageId >>> ', message['messageId'])
	print('receiptHandle >>> ', message['receiptHandle'])
	
	notification = json.loads(message['body'])
	textMessage = json.loads(notification['Message'])
	
	print('textMessage >>> ', textMessage)
	
	start(textMessage)

def lambda_handler(event, context):
	if event['Records'] and len(event['Records']) > 0:
		for message in event['Records']:
			process_message(message)
	# TODO implement
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}
