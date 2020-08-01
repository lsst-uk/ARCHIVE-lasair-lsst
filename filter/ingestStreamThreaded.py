"""Consumes stream for ingesting to database
"""
from __future__ import print_function
import argparse
import sys
import os
import time
import settings
import mysql.connector
import threading
import insert_query
import confluent_kafka
import json

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', type=str,
                        help='Hostname or IP of Kafka host to connect to.')

    parser.add_argument('--topic', type=str,
                        help='Name of Kafka topic to listen to.')

    parser.add_argument('--group', type=str,
                        help='Globally unique name of the consumer group. '
                        'Consumers in the same group will share messages '
                        '(i.e., only one consumer will receive a message, '
                        'as in a queue). Default is value of $HOSTNAME.')

    parser.add_argument('--maxalert', type=int,
                        help='Max alerts to be fetched per thread')

    parser.add_argument('--nthread', type=int,
                        help='Number of threads to use')

    args = parser.parse_args()
    return args

def make_database_connection():
    # Make a connection to the *local* database to put these
    msl = mysql.connector.connect(
        user     = settings.DB_USER_LOCAL, 
        password = settings.DB_PASS_LOCAL, 
        host     = settings.DB_HOST_LOCAL, 
        database = settings.DB_DATABASE,
        )
    return msl

def alert_filter(alert, msl):
    # Filter to apply to each alert.
    objectId = alert['objectId']
    if alert:
        # build the insert query for this object.
        # if not wanted, returns None
        query = insert_query.create_insert_query(alert)
        if query is None:
            return 0
        try:
            cursor = msl.cursor(buffered=True)
            cursor.execute(query)
            cursor.close()
        except mysql.connector.Error as err:
            print('INGEST object Database insert candidate failed: %s' % str(err))
        msl.commit()

        # now ingest the sherlock_classifications
        if 'annotations' in alert:
            annotations = alert['annotations']
            annClass = 'sherlock'
            if annClass in annotations:
                for ann in annotations[annClass]:
                    insert_query.create_insert_annotation(msl, objectId, annClass, ann, 
                        ['classification', 'description', 'summary', 'separation', 'z', 'catalogue_object_type'], 
                        'sherlock_classifications', replace=True)
    return 1

class Consumer(threading.Thread):
    """ Threaded consumer of kafka. Calls alert_filter() for each one
    """
    def __init__(self, threadID, args, conf):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.args = args

    def run(self):
        # Configure database connection
        msl = make_database_connection()
    
        # Start consumer and print alert stream
        try:
            consumer = confluent_kafka.Consumer(**self.conf)
            consumer.subscribe([self.args.topic])
        except Exception as e:
            print('INGEST Cannot start reader: %d: %s\n' % (self.threadID, e))
            return
    
        # Number of alerts in the batch
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        nalert = nalert_ingested = 0
        startt = time.time()
        while nalert < maxalert:
            # Here we get the next alert by kafka
            msg = consumer.poll(timeout=settings.KAFKA_TIMEOUT)
            if msg is None:
                break
            if msg.error():
                continue
            if msg.value() is None:
                continue
            else:
                # Apply filter to each alert
                alert = json.loads(msg.value())
                nalert += 1
                nalert_ingested += alert_filter(alert, msl)
                if nalert%1000 == 0:
                    print('thread %d nalert %d (ingested %d) time %.1f' % 
                        ((self.threadID, nalert, nalert_ingested, time.time()-startt)))
                    # refresh the database every 1000 alerts
                    # make sure everything is committed
                    msl.close()
                    msl = make_database_connection()
    
        print('INGEST %d finished with %d alerts (ingested %d)' % 
                (self.threadID, nalert, nalert_ingested))
        consumer.close()

def main():
    args = parse_args()

    # Configure consumer connection to Kafka broker
    conf = {
        'bootstrap.servers': '%s:9092' % args.host,
        'default.topic.config': {
             'auto.offset.reset': 'smallest'
        }}
    if args.group: conf['group.id'] = args.group
    else:          conf['group.id'] = 'LASAIR'
    print('Configuration = %s' % str(conf))

    # How many threads
    if args.nthread: nthread = args.nthread
    else:            nthread = 1
    print('Threads = %d' % nthread)

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, args, conf))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

if __name__ == '__main__':
    main()
