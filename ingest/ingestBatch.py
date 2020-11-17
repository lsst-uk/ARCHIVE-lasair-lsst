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
sys.path.append('../utility/')
from manage_status import manage_status
from confluent_kafka import Producer, KafkaError
import date_nid

def parse_args():
    """parse_args.
    """
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
    parser.add_argument('--avrodir', type=str,
                        help='Directory for blobs')
    args = parser.parse_args()
    return args

def msg_text(message):
    """msg_text. Remove postage stamp cutouts from an alert message.

    Args:
        message:
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text

def handle_alert(alert, store, producer, topicout):
    """handle_alert.
    Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert

    Args:
        alert:
        store:
        producer:
        topicout:
    """
    # here is the part of the alert that has no binary images
    nonimage = msg_text(alert)
    objectId = nonimage['objectId']

    if nonimage:  # Write your condition statement here
        # JSON version of the image-free alert
        s = json.dumps(nonimage, indent=2).encode()

        # do not put known solar system objects into kafka
        ss_mag = nonimage['candidate']['ssmagnr']
        if ss_mag > 0:
            return None

        # produce to kafka
        if producer is not None:
            try:
                producer.produce(topicout, s)
            except Exception as e:
                print("Kafka production failed for %s" % topicout)
                print(e)
        return objectId

class Consumer(threading.Thread):
    """Consumer.
    """

    # Threaded ingestion through this object
    def __init__(self, threadID, nalert_in_list, nalert_out_list, args, store, conf):
        """__init__.

        Args:
            threadID:
            nalert_list:
            args:
            store:
            conf:
        """
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.store = store
        self.args = args
        self.nalert_in_list = nalert_in_list
        self.nalert_out_list = nalert_out_list

    def run(self):
        """run.
        """
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
            topicout = None
    
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        nalert_in = 0
        nalert_out = 0
        startt = time.time()
        while nalert_in < maxalert:
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
            except alertConsumer.EopError as e:
                continue

            if msg is None:
                break
            else:
                for alert in msg:
                    # Apply filter to each alert
                    objectId = handle_alert(alert, self.store, producer, topicout)

                    if objectId:
                        self.store.putObject(objectId, streamReader.raw_msg)
                        nalert_out += 1

                    nalert_in += 1
                    if nalert_in%1000 == 0:
                        print('thread %d nalert %d time %.1f' % ((self.threadID, nalert_in, time.time()-startt)))
                        # if this is not flushed, it will run out of memory
                        if producer is not None:
                            producer.flush()
    
        # finally flush
        if producer is not None:
            producer.flush()
            print('kafka flushed')
        print('INGEST %d finished with %d alerts' % (self.threadID, nalert_in))
        self.nalert_in_list[self.threadID] = nalert_in
        self.nalert_out_list[self.threadID] = nalert_out
          
        streamReader.__exit__(0,0,0)

def main():
    """main.
    """
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

    store = objectStore.objectStore(suffix='avro', fileroot=args.avrodir)

    print('Configuration = %s' % str(conf))

    if args.nthread:
        nthread = args.nthread
    else:
        nthread = 1
    print('Threads = %d' % nthread)

    nalert_in_list = [0] * nthread
    nalert_out_list = [0] * nthread

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, nalert_in_list, nalert_out_list, args, store, conf))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

    nalert_in = sum(nalert_in_list)
    nalert_out = sum(nalert_out_list)
    os.system('date')
    ms = manage_status('nid', settings.SYSTEM_STATUS)
    nid  = date_nid.nid_now()
    ms.add({'today_ingest':nalert_in, 'today_ingest_out':nalert_out}, nid)

    if nalert_in > 0: return 1
    else:             return 0

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
