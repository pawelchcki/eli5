import Queue as queue
from csv_producer import CSVFileProducer
from elastic_consumer import ElasticConsumer
import threading
print CSVFileProducer, ElasticConsumer

data_queue = queue.Queue(1000)

producer = CSVFileProducer('./sample.csv')
consumer = ElasticConsumer('xxx2')

# consumer.create_index()

producer.launch(data_queue).start()
consumer.launch(data_queue).start()



