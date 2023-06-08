#
#   Textract custom parser - outputs a custom JSON to be parsed in Salesforce
#

import json
import boto3
from ast import keyword
from textractprettyprinter.t_pretty_print import Pretty_Print_Table_Format, Textract_Pretty_Print, get_string
from trp import Document
from trp.trp2 import TDocumentSchema
from trp.t_pipeline import order_blocks_by_geo

class TextractObject:
	FORMS_PO_KEYWORDS = ['customer po', 'cust po', 'customer po no.', 'customer po no', 'customer po #', 'po number', 'your po no.', 'po no',  'po #', 'Customer PO', 'Customer PO No', 'Customer PO No.', 'Customer PO #', 'Customer PO Number', 'p.o. no.', 'P.O. No.', 'customer order number']
	FORMS_INVOICE_KEYWORDS = ['invoice', 'invoice #', 'invoice number', 'invoice no', 'invoice no.', 'Invoice', 'Invoice #', 'Invoice Number', 'Invoice No', 'Invoice No.', 'invoice:', 'Invoice:', 'INVOICE:', 'i nvoi ce #', 'order number']
	
	DOC_MAP = {
		'invoice' : 'Invoice',
		'original invoice' : 'Invoice',
		'credit memo' : 'Invoice',
		'rental return' : 'Invoice',
		'credit order' : 'Invoice',
		'customer return' : 'Return',
		'packing list' : 'Packing Slip',
		'packing slip' : 'Packing Slip',
		'packing' : 'Packing Slip',
		'pick ticket' : 'Packing Slip',
		'quotation' : 'RFQ',
		'estimate' : 'RFQ',
		'sales quotation' : 'RFQ',
		'statement' : 'Statement',
		'monthly statement' : 'Statement',
		'weekly statement' : 'Statement',
		'quarterly statement' : 'Statement',
		'yearly statement' : 'Statement',
		'aged receivables' : 'Statement',
		'unidentified' : 'Unidentified',
		'acknowledgement' : 'Acknowledgement',
		'rfi' : 'RFI',
		'request for information' : 'RFI',
		'correspondence' : 'Correspondence',
		'submittal' : 'Submittal'
	}
	DOC_TYPES = DOC_MAP.keys()
	AWS_COMPREHEND_ARN = 'arn:aws:comprehend:us-east-1:973811007952:entity-recognizer/CustomEntityRecognizer/version/10'

	def __init__(self, response, output_path = '', pre_split = None):
		#  adding detected entities from Comprehend
		if len(response) > 0:
			client = boto3.client('comprehend')
			response = client.detect_entities(
				EndpointArn=self.AWS_COMPREHEND_ARN,
				Bytes=bytes(response, 'utf-8')
			)

		self.response = response
		self.output_path = output_path

		self.document = None

		self.num_split = 0
		self.split_arr = []

		self.pages_json = {}
		self.pages_split_reference = {}
		self.pages_confidence = {}

		self.tables_output_json = {}
		self.forms_output_json = {}

		self.is_parsed = False
		self.to_split = False

		self.po_key = ''
		self.po_value = ''
		self.invoice_key = ''
		self.invoice_value = ''
		
		if response != '' and response != None:
			try:
				# self.document = Document(response)

				t_doc = TDocumentSchema().load(response)
				print('loaded!')
				# the ordered_doc has elements ordered by y-coordinate (top to bottom of page)
				# ordered_doc = order_blocks_by_geo(t_doc)
				# send to trp for further processing logic
				self.document = Document(TDocumentSchema().dump(t_doc))
				print('dumped!')

				self.parseJSON()
				self.is_parsed = True
			except Exception as e:
				print('Error when parsing the JSON!')
				print(type(e))
				print(e.args)
				print(e)
		else:
			print('Response is formatted incorrectly!')
			print('Reponse : ' + response)
	
	def parseTables(self, page):
		json_output = {}

		# threshold on when to 
		header_threshold = 75

		# for averaging confidence
		total_confidence = 0
		row_count = 0

		# parsing tables data
		for table in page.tables:
			headersStr = ""
			
			if len(table.rows) > 0:
				for i in range(len(table.rows)):
					row = table.rows[i]

					isHeader = False
					sum_confidence = 0

					data = {}
					listCells = []
					listIds = []
					concatValue = ""
					
					# checking if first row/header
					if i == 0 or headersStr == "":
						isHeader = True

					for j in range(len(row.cells)):
						cell = row.cells[j]
						sum_confidence += cell.confidence
						text = cell.text.strip()
						id = cell.id
						
						# assuming first row is for headers
						if isHeader:
							concatValue += text + '|'
						else:
							listCells.append(text)
							listIds.append(id)

					# print('listCells >>> ', listCells)
					# print('listIds >>> ', listIds)

					rowConfidence = sum_confidence / len(row.cells)

					# for averaging confidence
					total_confidence += rowConfidence
					row_count += 1
					
					# removing the last delimiter for header str
					if isHeader:
						# print('header confidence >>> ', rowConfidence)

						if(rowConfidence > header_threshold):
							# print('accepted header!')
							if len(concatValue) > 0:
								headersStr = concatValue[0: len(concatValue) - 1]

							# print('headersStr >>> ', headersStr)
						else:
							print('row header fail confidence')
					# else, add row to final output
					else:
						data['avg_confidence'] = rowConfidence
						data['data'] = listCells
						data['ids'] = listIds

						# checking if header string is already in output map
						if headersStr not in json_output.keys():
							json_output[headersStr] = []

						json_output[headersStr].append(data)

		# print('tables json >>> ', json.dumps(json_output))

		json_output['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE'] = total_confidence / row_count if row_count > 0 else total_confidence

		return json_output

	def parseForms(self, page):
		json_output = {}

		# for averaging confidence
		total_confidence = 0
		row_count = 0

		po_key = ''
		po_value = ''
		po_index = -1
		invoice_key = ''
		invoice_value = ''
		invoice_index = -1

		# parsing forms data
		if page.form:
			for field in page.form.fields:
				key = ""
				value = ""
				key_confidence = 0
				value_confidence = 0
				
				if (field.key):
					key = field.key.text.strip().lower()
					key_confidence = field.key.confidence

					total_confidence += key_confidence
					row_count += 1
						
				if (field.value):
					value = field.value.text.strip()
					value_confidence = field.value.confidence

					total_confidence += value_confidence
					row_count += 1
					
				# print('key : {}, value : {}'.format(key, value))
				# print('key con : {}, value con : {}'.format(key_confidence, value_confidence))
				# print('field id : {}'.format(field.id))

				if key in self.FORMS_PO_KEYWORDS:
					if value != '' and (self.FORMS_PO_KEYWORDS.index(key) < po_index or po_index == -1):
						po_key = key
						po_value = value
						po_index = self.FORMS_PO_KEYWORDS.index(key)
				elif key != "" and key in self.FORMS_INVOICE_KEYWORDS:
					# print('invoice_key : {}, invoice_value : {}'.format(key, value))
					if value != '' and (self.FORMS_INVOICE_KEYWORDS.index(key) < invoice_index or invoice_index == -1):
						invoice_key = key
						invoice_value = value
						invoice_index = self.FORMS_INVOICE_KEYWORDS.index(key)
				
				if key not in json_output.keys():
					json_output[key] = {
						"confidence" : key_confidence,
						"data" : [
							{
								"value" : value,
								"confidence" : value_confidence
							}
						]
					}
				else:
					json_output[key]["data"].append({
							"value" : value,
							"confidence" : value_confidence
					})

		json_output['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE'] = total_confidence / row_count if row_count > 0 else total_confidence

		self.po_key = po_key
		self.po_value = po_value
		self.invoice_key = invoice_key
		self.invoice_value = invoice_value
		
		return json_output

	def mergeTables(self, init_json, new_json):
		print("merge tables!")
		for key in new_json.keys():
			if key in init_json.keys():
				init_json[key] = init_json[key] + new_json[key]
			else:
				init_json[key] = new_json[key]

	def mergeForms(self, init_json, new_json):
		print("merge forms!")
		for key in new_json.keys():
			if key in init_json.keys():
				init_json[key]["data"] = init_json[key]["data"] + new_json[key]["data"]
			else:
				init_json[key] = new_json[key]

	def exportJSONToFile(self):
		if len(self.pages_json.keys()) > 0:
			for key in self.pages_json.keys():
				output = self.pages_json[key]

				f = open(self.output_path + key + ".json", "w")
				f.write(json.dumps(output))
				f.close()

	def parseJSON(self):
		if self.document != None:
			doc = self.document
			page_num = 1

			# page key catch all if no specific key found
			page_key = ''

			for page in doc.pages:
				print('--------- Page Number ' + str(page_num) + ' -------------')
				forms_json = self.parseForms(page)
				tables_json = self.parseTables(page)

				list_confidence = []
				forms_confidence = forms_json['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE']
				tables_confidence = tables_json['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE']

				print('tables confidence >>> ' + str(forms_confidence))
				print('forms confidence >>> ' + str(tables_confidence))

				if forms_confidence > 0:
					list_confidence.append(forms_confidence)
				if tables_confidence > 0:
					list_confidence.append(tables_confidence)

				total_avg_confidence = (forms_json['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE'] + tables_json['TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE']) / len(list_confidence) if len(list_confidence) > 0 else 0

				forms_json.pop('TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE')
				tables_json.pop('TEXTRACT_RESULTS_TEMP_AVERAGE_CONFIDENCE')

				page_type = ''
				
				if(len(page.lines) > 0):
					for h in range(len(page.lines)):
						line_text = page.lines[h].text.strip().lower()

						# settings bounds to 25% of the document for finding the document type
						# this way it doesn't search the whole page
						geometry_bounds_max = .25
						bounds_top = page.lines[h].geometry.boundingBox.top

						if bounds_top > geometry_bounds_max:
							break

						if line_text in self.DOC_TYPES:
							print('type found : {}'.format(line_text))
							page_type = line_text
							break

				if page_type == '':
					page_type = 'unidentified'
					print('page type unidentified')


				#  REVAMP KEY START
				if self.invoice_value != '':
					print('invoice_value : ', self.invoice_value)
					page_key = self.invoice_value
				elif self.po_value != '':
					print('po_value : ', self.po_value)
					page_key = self.po_value
				else:
					print('No specific key found')
					if page_key == '':
						page_key = page_type

				#  OVERRIDE FOR DOCTYPES THAT FAVORS PO NUMBER majority we favor invoice number
				PO_DOCTYPES = ['Acknowledgement']

				doc_category = self.DOC_MAP[page_type]

				if doc_category in PO_DOCTYPES and self.po_value != '':
					page_key = self.po_value
				
				if page_key not in self.pages_json.keys():
					self.pages_json[page_key] = {
						"page_type" : doc_category,
						"tables" : {},
						"forms" : {}
					}
				else:
					if self.pages_json[page_key]["page_type"] != doc_category:
						page_key += '-' + doc_category

						self.pages_json[page_key] = {
							"page_type" : doc_category,
							"tables" : {},
							"forms" : {}
						}

				print('page_key >>> ' + page_key)

				self.mergeTables(self.pages_json[page_key]["tables"], tables_json)
				self.mergeForms(self.pages_json[page_key]["forms"], forms_json)

				if page_key not in self.pages_split_reference:
					self.pages_split_reference[page_key] = []

				self.pages_split_reference[page_key].append(page_num)
					
				if page_key not in self.pages_confidence:
					self.pages_confidence[page_key] = total_avg_confidence
				else:
					self.pages_confidence[page_key] = (self.pages_confidence[page_key] + total_avg_confidence) / 2
				#  REVAMP KEY END

				page_num += 1
				
			print('\n')
			print('Unique Pages >>> ' + str(len(self.pages_json.keys())))
			print('Page Keys >>> ' + str(self.pages_json.keys()))
			print('\n')
			print('Pages Split >>> ', self.pages_split_reference)
			print('Pages Confidence >>> ', self.pages_confidence)
	
		# self.exportJSONToFile()
		
			