import Queue as queue
from csv_producer import CSVStreamProducer
from solr_consumer import SolrConsumer
import threading, sys, time

data_queue = queue.Queue(1000)

producer = CSVStreamProducer(sys.stdin)

#if len(sys.argv) < 3:
#    consumer = ElasticConsumer('xxx2')
#else:

consumer = SolrConsumer(sys.argv[1], host=sys.argv[2])

# print sys.argv[1]
# sys.exit(0)

# consumer.create_index()

producer.launch(data_queue).start()
consumer.launch(data_queue).start()

no_one_is_working = 0
while True:
    if not (producer.is_working() or consumer.is_working()):
        no_one_is_working += 1
    else:
        no_one_is_working = 0

    if no_one_is_working > 10:
        sys.exit(0)
    time.sleep(1)


