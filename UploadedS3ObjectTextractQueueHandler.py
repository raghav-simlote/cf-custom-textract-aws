import os
import json
import boto3
from textractCustomParser.textract_parser import TextractObject

def parse_textract_result(jobId, s3Bucket, s3ObjectName):
	region = os.environ['AWS_REGION']
	textract = boto3.client('textract', region_name=region)
	
	mainResponse = None
	
	maxResults = 1000
	paginationToken = None
	finished = False
	
	#  For splitting response into pages
	jsonResDict = {}
	jsonPageCount = 0

	#  For signature workaround
	signatureDict = {}
	
	#  SNS ARNs
	sns_client = boto3.client('sns')
	sns_topicArn = 'arn:aws:sns:us-east-1:973811007952:AmazonSplitTextractQueue'
	sns_topicArn_dev = 'arn:aws:sns:us-east-1:973811007952:AmazonSplitTextractQueue_dev'
	
	mainResponse = { 'Blocks' : [] }
	
	while finished == False:
		response = None
	
		if paginationToken == None:
			response = textract.get_document_analysis(JobId=jobId, MaxResults=maxResults)
		else:
			response = textract.get_document_analysis(JobId=jobId, MaxResults=maxResults, NextToken=paginationToken)
			
		blocks = response['Blocks']

		for block in blocks:
			if block['BlockType'] == 'PAGE':
				jsonPageCount = jsonPageCount + 1
				print('Page count >>> ' + str(jsonPageCount))
				jsonResDict[jsonPageCount] = {'Blocks' : []}
				signatureDict[jsonPageCount] = False

			#  Workaround for Signature
			if block['BlockType'] == 'SIGNATURE':
				print('Signature found!')
				block_copy = block.copy()
				#  Need to replace Signature as LINE block type - parser throwing type error
				block_copy['BlockType'] = 'LINE'
				signatureDict[jsonPageCount] = True

				mainResponse['Blocks'].append(block_copy)
			else:
				mainResponse['Blocks'].append(block)

			jsonResDict[jsonPageCount]['Blocks'].append(block)
					
		if 'NextToken' in response:
			paginationToken = response['NextToken']
		else:
			finished = True
	
	# Saving main response of parent document
	s3Split = s3ObjectName.split('/')
	s3PathList = s3Split[0 : len(s3Split) - 1]
	s3Path = ''.join([str(item) + '/' for item in s3PathList])
	s3ObjectPath = s3Path + 's3.json'
	textractResultPath = s3Path + 'textract_result.json'
	textractPageResultPath = s3Path + 'pages/'
	
	baseURL = s3Split[0] + '/'
	parentId = s3Split[1]
	objectAPIName = s3Split[0]
	
	s3_client = boto3.client('s3')
	s3_client.put_object(Body=json.dumps(mainResponse), Bucket=s3Bucket, Key=textractResultPath)
	
	#  Getting Amazon S3 Attachment record information
	s3_obj = s3_client.get_object(Bucket=s3Bucket, Key=s3ObjectPath)
	s3_data = json.loads(s3_obj['Body'].read().decode('utf-8'))
	
	s3FileNameWithExtension = s3_data['File_Name__c']
	s3FileName = s3Split[-1].split('.')[0]
	s3FileNameExtension = '.' + s3Split[-1].split('.')[-1]
	#s3FileType = s3_data['File_Type__c']
	
	if len(jsonResDict.keys()) > 0:
		for key in jsonResDict.keys():
			s3_client.put_object(Body=json.dumps(jsonResDict[key]), Bucket=s3Bucket, Key=textractPageResultPath + str(key) + '.json')
	
	# Using custom parser
	parser = TextractObject(mainResponse, '')
	print('Parsing done with custom parser!')
	
	pages_json = parser.pages_json
	
	if len(pages_json.keys()) > 0:
		page_count = 1
		
		# Iterating through split pages of the extracted textract results
		for pkey in pages_json.keys():
			page = pages_json[pkey]
			pageType = page["page_type"]
			
			#  CO WORK AROUND FOR NOW
			if objectAPIName == 'Change_Order__c':
				pageType = 'Returned Change Order'
			
			if s3FileNameExtension == '.pdf':
				splitReference = parser.pages_split_reference[pkey]
			else:
				splitReference = None
				
			confidenceLevel = parser.pages_confidence[pkey]
				
			pkey_encoded = pkey.replace('/', '-')
			pkey_encoded = pkey_encoded.replace('%', '_')
			
			s3_client.put_object(Body=json.dumps(page), Bucket=s3Bucket, Key=textractPageResultPath + pkey_encoded + '.json')
			s3_client.put_object(Body=json.dumps(page["tables"]), Bucket=s3Bucket, Key=textractPageResultPath + pkey_encoded + '_tables.json')
			s3_client.put_object(Body=json.dumps(page["forms"]), Bucket=s3Bucket, Key=textractPageResultPath + pkey_encoded + '_forms.json')
			
			#  Checking if document has signature
			hasSignatures = False

			#  SplitReference is for multipage docs - if none, it's an image
			if splitReference != None:
				for split in splitReference:
					if split in signatureDict.keys() and signatureDict[split] == True:
						hasSignatures = True
						break
			else:
				if signatureDict[1] == True:
					hasSignatures = True
					
			#  INVOICE SIGNATURE CHECK - if Invoice and has Signatures, treat as Invoice with POD
			if pageType == 'Invoice' and hasSignatures == True:
				pageType = 'Invoice with POD'
			
			print('final pagetype >>>', pageType)

			jsonSplit = {
				'jobId' : jobId,
				's3Bucket' : s3Bucket, 
				's3ObjectName' : s3ObjectName,
				's3Path' : s3Path,
				'pKey' : pkey_encoded,
				'splitReference' : splitReference,
				'pageType' : pageType,
				'confidenceLevel' : confidenceLevel,
				'hasSignatures' : hasSignatures,
				'pages' : len(pages_json.keys())
			}
			
			if s3Bucket == 'horsepowernyc':
				response = sns_client.publish(
				    TopicArn= sns_topicArn,
				    Message= json.dumps({'default': json.dumps(jsonSplit)}),
				    MessageStructure= 'json'
				)
			elif s3Bucket == 'horsepower-uat':
				response = sns_client.publish(
				    TopicArn= sns_topicArn_dev,
				    Message= json.dumps({'default': json.dumps(jsonSplit)}),
				    MessageStructure= 'json'
				)
			else:
				response = sns_client.publish(
				    TopicArn= sns_topicArn_dev,
				    Message= json.dumps({'default': json.dumps(jsonSplit)}),
				    MessageStructure= 'json'
				)
				

def parse_document(message):
	print('message >>> ', message)
	print('messageId >>> ', message['messageId'])
	print('receiptHandle >>> ', message['receiptHandle'])
	
	notification = json.loads(message['body'])
	textMessage = json.loads(notification['Message'])
	
	jobId = textMessage['JobId']
	status = textMessage['Status']
	
	s3Info = textMessage['DocumentLocation']
	s3ObjectName = s3Info['S3ObjectName']
	s3Bucket = s3Info['S3Bucket']
	
	#print(jobId)
	#print(status)
	#print(s3ObjectName)
	#print(s3Bucket)
	#print(parentId)
	
	print('textMessage >>> ', textMessage)
	
	if status == 'SUCCEEDED':
		parse_textract_result(jobId, s3Bucket, s3ObjectName)
	

def lambda_handler(event, context):
	
	if event['Records'] and len(event['Records']) > 0:
		for message in event['Records']:
			parse_document(message)
	 
	# TODO implement
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}