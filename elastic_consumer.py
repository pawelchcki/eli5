import __future__

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from  elasticsearch import helpers as elhelpers
import common, time, json, sys
import datetime as dt

class ElasticConsumer(common.QueueProcessor):	
	shard_count = 3
	replica_count = 1
	timeout = 1024
	def __init__(self, index_name, doc_type = 'test', host = 'localhost', port = 9200):
		self.es = Elasticsearch([
		    {'host': host, 'port': port},
		])
		self.heartbeat_state = 0
		self.last_status = dt.datetime.now()
		self.total_docs = 0
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

	def visual_heartbeat(self, batch):
		now = dt.datetime.now()
		self.total_docs += len(batch)
		if (now - self.last_status).seconds > 2:
			self.last_status = now
			print self.total_docs
	

		if self.heartbeat_state == 0:
			sys.stdout.write('|')
			sys.stdout.flush()
			self.heartbeat_state = 1
		elif self.heartbeat_state == 1:
			sys.stdout.write("\b")
			sys.stdout.write('-')
			sys.stdout.flush()
			self.heartbeat_state = 2
		elif self.heartbeat_state == 2:
			sys.stdout.write("\b")
			sys.stdout.write('|')
			sys.stdout.flush()
			self.heartbeat_state = 1
		else: 
			self.heartbeat_state = 1 


	def send_batch(self, batch, max_tries = 10):
		dressed_batch = map(lambda body: {'_index': self.index_name, '_type': self.doc_type, '_source': body}, batch)
		tried = 0

		while tried < max_tries:
			try:
				self.visual_heartbeat(batch)
				elhelpers.bulk(self.es, dressed_batch)
				break
			except ConnectionError as e:
				print(e)
				wait_time = 2**tried
				tried += 1
				print("Waiting %ss before retrying request" % wait_time)
				time.sleep(wait_time)
			except Exception as e:
				print(e)
				tried += 1
				time.sleep(10)

	def process(self, data_queue):
		try:		
			while True:
				self.stop_work()
				batch = data_queue.get(True, self.timeout)
				self.start_work()
				self.send_batch(batch)
		finally:
			self.stop_work()

