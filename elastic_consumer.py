import __future__

from elasticsearch import Elasticsearch
from  elasticsearch import helpers as elhelpers
import common, time

class ElasticConsumer(common.QueueProcessor):	
	shard_count = 3
	replica_count = 1
	timeout = 1024
	def __init__(self, index_name, doc_type = 'test', host = 'localhost', port = 9200):
		self.es = Elasticsearch([
		    {'host': host, 'port': port},
		])

		self.index_name = index_name
		self.doc_type = doc_type

	def create_index(self, mapping=None):
		if not mapping:
			mapping_file = file('./mapping.json', 'r')
			# print mapping_file.read()
			mapping = json.load(mapping_file)		
		self.mapping = mapping
		settings = {
			'number_of_shards': self.shard_count,
			'number_of_replicas': self.replica_count
		}
		self.es.indices.create(self.index_name, {'settings': settings, 'mappings': self.mapping})

	def send_batch(self, batch, max_tries = 10):
		dressed_batch = map(lambda body: {'_index': self.index_name, '_type': self.doc_type, '_source': body}, batch)
		tried = 0
		while tried < max_tries:
			try:
				elhelpers.bulk(self.es, dressed_batch)
				break
			except ConnectionError as e:
				print(e)
				wait_time = 2**tried
				tried += 1
				print("Waiting %ss before retrying request" % tried)
				time.sleep(wait_time)


	def process(self, data_queue):		
		while True:
			batch = data_queue.get(True, self.timeout)
			self.send_batch(batch)

