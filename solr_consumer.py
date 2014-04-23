import __future__

import common, time, json, sys
import random
import datetime as dt
import solr


class SolrConsumer(common.QueueProcessor):
    shard_count = 3
    replica_count = 1
    timeout = 1024
    url = 'http://%s:%i/solr/%s'

    def __init__(self, index_name, doc_type='test', host='dev-search-s4', port=8983):
        self.solr = solr.SolrConnection(self.url % (host, port, index_name), )
        self.heartbeat_state = 0
        self.last_status = dt.datetime.now()
        self.total_docs = 0
        self.index_name = index_name
        self.doc_type = doc_type

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

    def randomize_batch(self, batch):
        for doc in batch:
            doc['id'] = "%i_%i_%i" % (random.randint(0, 100000), random.randint(0, 1000000), random.randint(0, 1000000))
            arr = doc['html_' + doc['lang']].split(" ")
            random.shuffle(arr)
            doc['html_' + doc['lang']] = " ".join(arr)
            arr = doc['nolang_txt'].split(" ")
            random.shuffle(arr)
            doc['nolang_txt'] = " ".join(arr)

        return batch

    def send_batch(self, batch, max_tries=10):
        batch = self.randomize_batch(batch)
        tried = 0
        while tried < max_tries:
            try:
                self.visual_heartbeat(batch)
                self.solr.add_many(batch)
                self.solr.commit()
                break
            except solr.SolrException as e:
                print(e)
                wait_time = 2 ** tried
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

