import Queue as queue
from csv_producer import CSVStreamProducer
import threading, sys, time
import getopt


def usage():
    print ""
    print "USAGE: gzip -d < dump.gz | python ~/eli5/enlarger.py --index=main --host=db-sds-s3"
    print ""
    print "OPTIONS:"
    print "--index=xxx - index name on elastic search / core name on solr"
    print "--host - host name"
    print "--queue=1000 - queue size"
    print "--limit=0 - limit the number of lines from the CSV file"
    print "--offset=0 - omit the number of lines from the CSV file"
    print "--workers=1 - how many threads"
    print "--engine=solr|elastic"
    print ""


params = {'index': '',
          'host': '',
          'queue': 1000,
          'limit': 0,
          'offset': 0,
          'workers': 1,
          'batchsize': 100,
          'engine': 'solr'}


def workers_working(workers):
    for worker in workers:
        if worker.is_working():
            return True
    return False

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "", ["help", "index=", "host=", "queue=", "limit=", "offset=", "workers=", "batchsize=", "engine="])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-i", "--index"):
            params['index'] = arg
        elif opt in ("-h", "--host"):
            params['host'] = arg
        elif opt in ("-q", "--queue"):
            params['queue'] = arg
        elif opt in ("-l", "--limit"):
            params['limit'] = arg
        elif opt in ("-o", "--offset"):
            params['offset'] = arg
        elif opt in ("-w", "--workers"):
            params['workers'] = arg
        elif opt in ("-b", "--batchsize"):
            params['batchsize'] = arg
        elif opt in ("-e", "--engine"):
            params['engine'] = arg

    if params['engine'] == 'solr':
        from solr_consumer import SolrConsumer as Consumer
    else:
        from elastic_consumer import ElasticConsumer as Consumer

    if params['index'] == '' or params['host'] == '':
        usage()
        sys.exit(2)

    data_queue = queue.Queue(int(params['queue']))

    producer = CSVStreamProducer(sys.stdin, int(params['limit']), int(params['offset']), batch_size=int(params['batchsize']))
    consumers = []
    for num in range(0, int(params['workers'])):
        consumers.append(Consumer(params['index'], host=params['host']))

    producer.launch(data_queue).start()
    for consumer in consumers:
        consumer.launch(data_queue).start()

    no_one_is_working = 0

    while True:
        if not (producer.is_working() or workers_working(consumers)):
            no_one_is_working += 1
        else:
            no_one_is_working = 0

        if no_one_is_working > 10:
            sys.exit(0)
        time.sleep(1)

if __name__ == "__main__":
    main(sys.argv[1:])