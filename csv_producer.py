import csv, sys
import common
from itertools import izip, ifilter, imap

csv.field_size_limit(sys.maxsize)


class CSVFileProducer(common.QueueProcessor):
    delimiter = ','
    quotechar = '"'
    batch_size = 100
    timeout = 1024


    def __init__(self, filepath, limit=0, offset=0):
        self.init_source(filepath)
        self.processed = 0
        self.limit = limit
        self.offset = offset

    def init_source(self, filepath):
        source_file = open(filepath, 'r')
        self.init_csv(source_file)

    def init_csv(self, source):
        self.csv_reader = csv.reader(source, delimiter=self.delimiter, quotechar=self.quotechar)
        self.head_row = self.csv_reader.next()

    def parse_row(self, row):
        utf8_row = imap(lambda item: item.decode('utf-8'),row)
        not_filtered = izip(self.head_row, utf8_row)

        # print dict(ifilter(lambda pair: pair[0] != "_version_", not_filtered))
        return dict(ifilter(lambda pair: pair[0] != "_version_" and len(pair[1]) > 0, not_filtered))

    def process_input(self):
        for row in self.csv_reader:
            yield self.parse_row(row)
            if self.processed >= self.offset:
                yield self.parse_row(row)

            self.processed += 1

            if 0 < self.limit <= self.processed:
                break

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
            self.stop_work()
            data_queue.put(batch, True, self.timeout)
            self.start_work()
        self.stop_work()


class CSVStreamProducer(CSVFileProducer):
    def init_source(self, in_file):
        self.init_csv(iter(in_file.readline, ''))
