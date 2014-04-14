import csv
import common

class CSVFileProducer(common.QueueProcessor):	
	delimiter = ','
	quotechar = '"'
	batch_size = 100
	timeout = 60
	def __init__(self, filepath):
		self.init_source(filepath)

	def init_source(self, filepath):
		self.source_file = open(filepath, 'r')
		self.csv_reader = csv.reader(self.source_file,
		 delimiter=self.delimiter, quotechar=self.quotechar)
		self.head_row = self.csv_reader.next()

	def parse_row(self, row):
		dict = {}
		map(lambda k, v: dict.update({k: v}) if v != '' else None, self.head_row, row)
		return dict		
		
	def process_input(self):
		for row in self.csv_reader:
			yield self.parse_row(row)

	def batch_process_input(self, batch_size=100):
		batch = []
		for row in self.process_input():
			batch.append(row)
			if len(batch) > batch_size:
				yield batch
				batch = []

		if len(batch) > 0:
			yield batch

	def process(self, data_queue):
		for batch in self.batch_process_input(self.batch_size):			
			data_queue.put(batch, True, self.timeout)			
