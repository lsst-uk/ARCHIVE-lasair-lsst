"""Consumes stream for ingesting to database
"""

from __future__ import print_function
import argparse
import sys
import os
import time
import settings
import threading
import alertConsumer
import objectStore
import json
from confluent_kafka import Producer, KafkaError

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topic', type=str,
                        help='Name of Kafka topic to listen to.')
    parser.add_argument('--topicout', type=str,
                        help='Name of Kafka topic to send out alerts.')
    parser.add_argument('--group', type=str,
                        help='Globally unique name of the consumer group. '
                        'Consumers in the same group will share messages '
                        '(i.e., only one consumer will receive a message, '
                        'as in a queue). Default is value of $HOSTNAME.')
    parser.add_argument('--maxalert', type=int,
                        help='Max alerts to be fetched per thread')
    parser.add_argument('--nthread', type=int,
                        help='Number of threads to use')
    parser.add_argument('--stampdir', type=str,
                        help='Directory for blobs')
    args = parser.parse_args()
    return args

def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text

def write_stamp_file(stamp_dict, store):
    """Given a stamp dict that follows the cutout schema,
       write data to a file in a given directory.
    """

    """ examples of this file name 
candid1189406621015015005_pid1189406621015_targ_scimref.fits.gz
candid1189406621015015005_ref.fits.gz
candid1189406621015015005_pid1189406621015_targ_sci.fits.gz
    """
    f = stamp_dict['fileName']
    candid = f.split('_')[0]
    store.putObject(f, stamp_dict['stampData'])
    return

def handle_alert(alert, store, producer, topicout):
    """Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert
    """

    # here is the part of the alert that has no binary images
    nonimage = msg_text(alert)

    if nonimage:  # Write your condition statement here

        # write the stamps to the object store
        if 'fits' in store:
            write_stamp_file( alert.get('cutoutDifference'), store['fits'])
            write_stamp_file( alert.get('cutoutTemplate'),   store['fits'])
            write_stamp_file( alert.get('cutoutScience'),    store['fits'])

        # JSON version of the image-free alert
        s = json.dumps(nonimage, indent=2).encode()

        # replace existing lightcurve with this one
        if 'lightcurve' in store:
            slc = store['lightcurve']
            objectId = nonimage['objectId']
            slc.putObject(objectId, s)

        # produce to kafka
        if producer is not None:
            try:
                producer.produce(topicout, s)
            except Exception as e:
                print("Kafka production failed for %s" % topicout)
                print(e)

class Consumer(threading.Thread):
    # Threaded ingestion through this object
    def __init__(self, threadID, args, store, conf):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.store = store
        self.args = args

    def run(self):
        try:
            streamReader = alertConsumer.AlertConsumer(self.args.topic, **self.conf)
            streamReader.__enter__()
        except alertConsumer.EopError as e:
            print('INGEST Cannot start reader: %d: %s\n' % (self.threadID, e.message))
            return

        # if we are doing a kafka output
        if self.args.topicout:
            conf = {
                'bootstrap.servers': '%s' % settings.KAFKA_OUTPUT,
                'group.id': 'copy-topic',
                'client.id': 'client-1',
                'enable.auto.commit': True,
                'session.timeout.ms': 6000,
                'default.topic.config': {'auto.offset.reset': 'smallest'}
            }
            producer = Producer(conf)
            topicout = self.args.topicout
            print('Producing Kafka to %s with topic %s' % (settings.KAFKA_OUTPUT, topicout))
        else:
            producer = None
            kafkaout = None
    
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
                break
            else:
                for alert in msg:
                    # Apply filter to each alert
                    candid = handle_alert(alert, self.store, producer, topicout)
                    nalert += 1
                    if nalert%1000 == 0:
                        print('thread %d nalert %d time %.1f' % ((self.threadID, nalert, time.time()-startt)))
                        # if this is not flushed, it will run out of memory
                        if producer is not None:
                            producer.flush()
    
        # finally flush
        if producer is not None:
            producer.flush()
            print('kafka flushed')
        print('INGEST %d finished with %d alerts' % (self.threadID, nalert))

          
        streamReader.__exit__(0,0,0)

def main():
    args = parse_args()

    if args.group: groupid = args.group
    else:          groupid = 'LASAIR'

    conf = {
        'bootstrap.servers': '%s' % settings.KAFKA_INPUT,
        'group.id': groupid,
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    store = {
        'fits'      : objectStore.objectStore(
            suffix='fits.gz', fileroot=args.stampdir + '/fits'),

        'lightcurve': objectStore.objectStore(
            suffix='json', fileroot=args.stampdir + '/lightcurve'),
    }

    print('Configuration = %s' % str(conf))

    if args.nthread:
        nthread = args.nthread
    else:
        nthread = 1
    print('Threads = %d' % nthread)

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, args, store, conf))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

if __name__ == '__main__':
    main()
