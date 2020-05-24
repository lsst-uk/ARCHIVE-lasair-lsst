"""Consumes stream for ingesting to database

"""

from __future__ import print_function
import argparse
import sys
import os
import time
import settings
import mysql.connector
from mag import dc_mag
import threading
import alertConsumer
import make_query
import json

def alert_filter(alert, msl):
    """Filter to apply to each alert.
    """
    candid = 0
    data = alert
    if alert:
        query = create_query.create_insert_query(alert)
        try:
            cursor = msl.cursor(buffered=True)
            cursor.execute(query)
            cursor.close()
        except mysql.connector.Error as err:
            print('INGEST Database insert candidate failed: %s' % str(err))
        msl.commit()

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
    msl = mysql.connector.connect(
        user     = settings.DB_USER_WRITE, 
        password = settings.DB_PASS_WRITE, 
        host     = settings.DB_HOST_LOCAL, 
        database = settings.DB_DATABASE,
        )
    return msl

class Consumer(threading.Thread):
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
            streamReader = alertConsumer.AlertConsumer(self.args.topic, **self.conf)
            streamReader.__enter__()
        except alertConsumer.EopError as e:
            print('INGEST Cannot start reader: %d: %s\n' % (self.threadID, e.message))
            return
    
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        nalert = 0
        startt = time.time()
        while nalert < maxalert:
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
            except alertConsumer.EopError as e:
                continue

            if msg is None:
                continue
            else:
                for record in msg:
                    # Apply filter to each alert
                    alert = json.loads(record)
                    alert_filter(alert, msl)
                    nalert += 1
                    if nalert%1000 == 0:
                        print('thread %d nalert %d time %.1f' % ((self.threadID, nalert, time.time()-startt)))
                        msl.close()
                        msl = make_database_connection()
    
        print('INGEST %d finished with %d alerts' % (self.threadID, nalert))

        streamReader.__exit__(0,0,0)

def main():
    args = parse_args()

    # Configure consumer connection to Kafka broker
    conf = {'bootstrap.servers': '{}:9092'.format(args.host,args.host,args.host),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    if args.group: conf['group.id'] = args.group
    else:          conf['group.id'] = 'LASAIR'

    print('Configuration = %s' % str(conf))

    if args.nthread:
        nthread = args.nthread
    else:
        nthread = 1
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
