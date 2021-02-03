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
sys.path.append('../utility/')
import date_nid
from manage_status import manage_status

sherlock_attributes = [
    "classification",
    "objectId",
    "association_type",
    "catalogue_table_name",
    "catalogue_object_id",
    "catalogue_object_type",
    "raDeg",
    "decDeg",
    "separationArcsec",
    "northSeparationArcsec",
    "eastSeparationArcsec",
    "physical_separation_kpc",
    "direct_distance",
    "distance",
    "z",
    "photoZ",
    "photoZErr",
    "Mag",
    "MagFilter",
    "MagErr",
    "classificationReliability",
    "annotator",
    "additional_output",
    "description",
    "summary",
]

def parse_args():
    """parse_args.
    """
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
    """make_database_connection.
    """
    # Make a connection to the *local* database to put these
    msl = mysql.connector.connect(
        user     = settings.DB_USER_LOCAL, 
        password = settings.DB_PASS_LOCAL, 
        host     = settings.DB_HOST_LOCAL, 
        database = settings.DB_DATABASE,
        )
    return msl

def alert_filter(alert, msl):
    """alert_filter.

    Args:
        alert:
        msl:
    """
    # Filter to apply to each alert.
    objectId = alert['objectId']

    # run the cassandra process
    ncandidates = insert_query.insert_cassandra(alert)

    # build the insert query for this object.
    # if not wanted, returns None
    query = insert_query.create_insert_query(alert)
    if query is None:
        return {'objects':0, 'candidates':ncandidates} 
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
    except mysql.connector.Error as err:
        print('INGEST object Database insert candidate failed: %s' % str(err))
        print(query)
    msl.commit()

    # now ingest the sherlock_classifications
    if 'annotations' in alert:
        annotations = alert['annotations']
        annClass = 'sherlock'
        if annClass in annotations:
            for ann in annotations[annClass]:
                if "transient_object_id" in ann:  # hack here. Sherlock and Lasair have different names
                    ann['objectId'] = ann.pop('transient_object_id')

                insert_query.create_insert_annotation(msl, objectId, annClass, ann, 
                    sherlock_attributes, 'sherlock_classifications', replace=True)
    return {'objects':1, 'candidates':ncandidates} 

class Consumer(threading.Thread):
    """ Threaded consumer of kafka. Calls alert_filter() for each one
    """
    def __init__(self, threadID, nalert_in_list, nalert_out_list, ncandidates_out_list, args, conf):
        """__init__.

        Args:
            threadID:
            nalert_in_list:
            nalert_out_list:
            ncandidates_out_list:
            args:
            conf:
        """
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.nalert_in_list       = nalert_in_list
        self.nalert_out_list      = nalert_out_list
        self.ncandidates_out_list = ncandidates_out_list
        self.conf = conf
        self.args = args

    def run(self):
        """run.
        """
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
    
        nalert_in = nalert_out = ncandidates_out = 0
        startt = time.time()
        while nalert_in < maxalert:
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
                nalert_in += 1
                output_dict = alert_filter(alert, msl)
                nalert_out += output_dict['objects']
                ncandidates_out += output_dict['candidates']
                if nalert_in%1000 == 0:
                    print('thread %d nalert_in %d nalert_out %d ncandidates_out %d time %.1f' % 
                        ((self.threadID, nalert_in, nalert_out, ncandidates_out, time.time()-startt)))
                    # refresh the database every 1000 alerts
                    # make sure everything is committed
                    msl.close()
                    msl = make_database_connection()
    
        consumer.close()
        self.nalert_in_list[self.threadID] = nalert_in
        self.nalert_out_list[self.threadID] = nalert_out
        self.ncandidates_out_list[self.threadID] = ncandidates_out

def main():
    """main.
    """
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

    # number of alerts from each
    nalert_in_list = [0] * nthread
    nalert_out_list = [0] * nthread
    ncandidates_out_list = [0] * nthread

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, nalert_in_list, nalert_out_list, ncandidates_out_list, args, conf))

    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

    nalert_in = sum(nalert_in_list)
    nalert_out = sum(nalert_out_list)
    ncandidates_out = sum(ncandidates_out_list)
    print('INGEST finished %d in, %d out, %d candidates' % (nalert_in, nalert_out, ncandidates_out))

    ms = manage_status('nid', settings.SYSTEM_STATUS)
    nid  = date_nid.nid_now()
    ms.add({'today_filter':nalert_in, 'today_filter_out':nalert_out, 'today_candidates_out':ncandidates_out}, nid)

    if nalert_in > 0: return 1
    else:             return 0

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
