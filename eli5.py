import Queue as queue
from csv_producer import CSVStreamProducer
from elastic_consumer import ElasticConsumer
import threading, sys 

data_queue = queue.Queue(1000)

producer = CSVStreamProducer(sys.stdin)
consumer = ElasticConsumer('xxx2')

# consumer.create_index()

producer.launch(data_queue).start()
consumer.launch(data_queue).start()



