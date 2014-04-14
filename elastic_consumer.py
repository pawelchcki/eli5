from elasticsearch import Elasticsearch
import json

class ElasticConsumer:	
	def __init__(self, db_name, host = localhost, port = 9100):
		self.es = Elasticsearch([
		    {'host': host, 'port': port},
		])
		self.db_name = db_name

	def setup_mapping(self, mapping=None):
		if not mapping:
			mapping_file = file('./mapping.json', 'r')
			mapping = json.load(mapping_file)
		print mapping



 
	def consume(self, data_queue):		
		data_queue.get	

e = ElasticConsumer('x')
e.setup_mapping()