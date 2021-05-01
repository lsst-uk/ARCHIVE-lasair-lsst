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
import zlib
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
    parser.add_argument('--objectdir', type=str,
                        help='Directory for objects')
    parser.add_argument('--fitsdir', type=str,
                        help='Directory for fits images')
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

def store_images(message, store, candid):
    for cutoutType in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']:
        contentgz = message[cutoutType]['stampData']
        content = zlib.decompress(contentgz, 16+zlib.MAX_WBITS)
        filename = '%d_%s' % (candid, cutoutType)
        store.putObject(filename, content)

def insert_cassandra(alert):
    """insert_casssandra.
    Creates an insert for cassandra
    a query for inserting it.

    Args:
        alert:
    """

    # if this is not set, then we are not doing cassandra
    try:
        if len(settings.CASSANDRA_HEAD) == 0: return 0
    except:
        return 0

    # if it does not have all the ZTF attributes, don't try to ingest
    if not 'candid' in alert['candidate'] or not alert['candidate']['candid']:
        return 0

    from cassandra.cluster import Cluster

    objectId =  alert['objectId']

    candlist = None
    # Make a list of candidates and noncandidates in time order
    if 'candidate' in alert and alert['candidate'] != None:
        if 'prv_candidates' in alert and alert['prv_candidates'] != None:
            candlist = alert['prv_candidates'] + [alert['candidate']]
        else:
            candlist = [alert['candidate']]

    # will be list of real detections, each has a non-null candid
    detectionCandlist = []
    nondetectionCandlist = []

    # 2021-03-01 KWS Issue 134: Add non detections.
    for cand in candlist:
        cand['objectId'] = objectId
        if not 'candid' in cand or not cand['candid']:
            # This is a non-detection. Just append the subset of attributes we want to keep.
            # The generic cassandra inserter should be able to insert correctly based on this.
            nondetectionCandlist.append({'objectId': cand['objectId'],
                                         'jd': cand['jd'],
                                         'fid': cand['fid'],
                                         'diffmaglim': cand['diffmaglim'],
                                         'nid': cand['nid'],
                                         'field': cand['field'],
                                         'magzpsci': cand['magzpsci'],
                                         'magzpsciunc': cand['magzpsciunc'],
                                         'magzpscirms': cand['magzpscirms']})
        else:
            detectionCandlist.append(cand)

    if len(detectionCandlist) == 0 and len(nondetectionCandlist) == 0:
        # No point continuing. We have no data.
        return 0

    # connect to cassandra cluster
    try:
        cluster = Cluster(settings.CASSANDRA_HEAD)
        session = cluster.connect()
        session.set_keyspace('lasair')
    except Exception as e:
        print("Cassandra connection failed for %s" % str(settings.CASSANDRA_HEAD))
        print(e)
        return 0

    if len(detectionCandlist) > 0:
        # Add the htm16 IDs in bulk. Could have done it above as we iterate through the candidates,
        # but the new C++ bulk code is 100 times faster than doing it one at a time.
        # Note that although we are inserting them into cassandra, we are NOT using
        # HTM indexing inside Cassandra. Hence this is a redundant column.
        htm16s = htmCircle.htmIDBulk(16, [[x['ra'],x['dec']] for x in detectionCandlist])

        # Now add the htmid16 value into each dict.
        for i in range(len(detectionCandlist)):
            detectionCandlist[i]['htmid16'] = htm16s[i]

        cassandra_import.loadGenericCassandraTable(session, \
                settings.CASSANDRA_CANDIDATES, detectionCandlist)

    if len(nondetectionCandlist) > 0:
        cassandra_import.loadGenericCassandraTable(session, \
                settings.CASSANDRA_NONCANDIDATES, nondetectionCandlist)


    cluster.shutdown()
    return len(detectionCandlist)

candidate_attributes = [
'candid',
'dec',
'drb',
'diffmaglim',
'fid',
'field',
'isdiffpos',
'jd',
'magnr',
'magpsf',
'magzpsci',
'neargaia',
'neargaiabright',
'nid',
'objectidps1',
'ra',
'rb',
'sgmag1',
'srmag1',
'sgscore1',
'distpsnr1',
'sigmagnr',
'sigmapsf',
'ssdistnr',
'ssmagnr',
'ssnamenr',
]

def extract(candidate):
    # get just what we want 
    newcan = {}
    for ca in candidate_attributes:
        if ca in candidate:
            c = candidate[ca]
            if c: newcan[ca] = c
    return newcan

def join_old_and_new(alert, old):
    """join_old_and_new.
    Args:
        alert:
        old:
    """
    # here is the part of the alert that has no binary images
    objectId = alert['objectId']

    # this will be the new version of the object
    new = {}
    new['objectId'] = objectId
    allcandidates = []

    # put in the list what just came in
    allcandidates.append(extract(alert['candidate']))
    if 'prv_candidates' in alert and alert['prv_candidates']:
        for p in alert['prv_candidates']:
            allcandidates.append(extract(p))

    if old:
        allcandidates += old['candidates']
        if 'prv_candidates' in old:
            allcandidates += old['noncandidates']

    # sort by jd
    allcandidates = sorted(allcandidates, key=lambda c:c['jd'], reverse=True)
    candidates = []
    noncandidates = []
    for c in allcandidates:
        if 'candid' in c and c['candid']:
            candidates.append(c)
        else:
            noncandidates.append(c)

    new['candidates'] = candidates
    new['noncandidates'] = noncandidates
    return new

def handle_alert(alert, json_store, image_store, producer, topicout):
    """handle_alert.
    Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert

    Args:
        alert:
        json_store:
        image_store:
        producer:
        topicout:
    """
    # here is the part of the alert that has no binary images
    alert_noimages = msg_text(alert)



    # Call on Cassandra
    insert_cassandra(alert_noimages)





    candid = alert_noimages['candidate']['candid']

    if not alert_noimages:
        return None

    objectId = alert_noimages['objectId']
    # fetch the stored version of the object
    jold = json_store.getObject(objectId)
    if jold:
        old = json.loads(jold)
#        print('--- old ---', old)   #####
    else:
        old = None
#    print('--- alert ---', alert_noimages)   #####
    new = join_old_and_new(alert_noimages, old)
#    print('--- new ---', json.dumps(new, indent=2))   #####

    # store the new version of the object
    new_object_json = json.dumps(new, indent=2)
    json_store.putObject(objectId, new_object_json)

    # store the fits images
    store_images(alert, image_store, candid)

    # do not put known solar system objects into kafka
    ss_mag = alert_noimages['candidate']['ssmagnr']
    if ss_mag > 0:
        return None

        # produce to kafka
    if producer is not None:
        try:
            s = json.dumps(alert_noimages)
            producer.produce(topicout, json.dumps(alert_noimages))
        except Exception as e:
            print("Kafka production failed for %s" % topicout)
            print(e)
    return objectId

class Consumer(threading.Thread):
    """Consumer.
    """

    # Threaded ingestion through this object
    def __init__(self, threadID, nalert_in_list, nalert_out_list, args, json_store, image_store, conf):
        """__init__.

        Args:
            threadID:
            nalert_list:
            args:
            json_store:
            image_store:
            conf:
        """
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.json_store = json_store
        self.image_store = image_store
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
            t = time.time()
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
#                t = time.time() -t
#                print('%.3f %d' % (t, nalert_in))
            except alertConsumer.EopError as e:
#                t = time.time() -t
#                print('%.3f eop %d' % (t, nalert_in))
#                continue
                print('eop end of messages')
                break

            if msg is None:
                print('null message end of messages')
                break
            else:
                for alert in msg:
                    # Apply filter to each alert
                    objectId = handle_alert(alert, self.json_store, self.image_store, producer, topicout)

                    if objectId:
                        nalert_out += 1

                    nalert_in += 1
                    if nalert_in%5000 == 0:
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

    json_store = objectStore.objectStore(suffix='json', fileroot=args.objectdir)
    image_store  = objectStore.objectStore(suffix='fits', fileroot=args.fitsdir)

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
        thread_list.append(Consumer(t, nalert_in_list, nalert_out_list, args, json_store, image_store, conf))
    
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
