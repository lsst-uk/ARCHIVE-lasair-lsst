"""Sherlock Kafka wrapper

Consumes alerts (in plain JSON) from an input topic, sends them to Sherlock,
adds the Sherlock classification and crossmatches back into the alert and
republishes on the output topic.
"""

import json
import yaml
import argparse
import logging
import sys
from urllib.parse import urlparse
import pymysql.cursors
from confluent_kafka import Consumer, Producer, KafkaError
#from mock_sherlock import transient_classifier
from sherlock import transient_classifier

# TODO replace with a proper queue(s) for multi-threading?
#alerts = {}

def consume(conf, log, alerts, consumer=None):
    "fetch a batch of alerts from kafka, return number of alerts consumed"

    #global alerts

    log.debug('called consume with config: ' + str(conf))
    
    # if we haven't been given a consumer then create one
    if consumer == None:
        # Kafka settings
        settings = {
            'bootstrap.servers': conf['broker'],
            'group.id': conf['group'],
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'enable.auto.commit': False
        }
        c = Consumer(settings, logger=log)
    else:
        c = consumer

    c.subscribe([conf['input_topic']])

    n = 0
    n_error = 0
    try:
        while n < conf['batch_size']:
            # Poll for messages
            msg = c.poll(conf['timeout'])
            if msg is None:
                # stop when we get to the end of the topic
                # TODO make this optional?
                log.info('reached end of topic')
                break
            elif not msg.error():
                log.debug("Got message with offset " + str(msg.offset()))
                alert = json.loads(msg.value())
                #name = alert.get('objectId', alert.get('candid'))
                #alerts[name] = alert
                alerts.append(alert)
                n += 1
            else:
                n_error += 1
                log.warning(str(msg.error()))
                try:
                    if msg.error().fatal():
                        break
                except:
                    pass
                if conf['max_errors'] < 0:
                    continue
                elif conf['max_errors'] < n_error:
                    log.error("maximum number of errors reached")
                    break
                else:
                    continue
        log.info("consumed {:d} alerts".format(n))
        if n > 0:
            n_classified = classify(conf, log, alerts)
            if n_classified != n:
                raise Exception("Failed to classify all alerts in batch: expected {}, got {}".format(n, n_classified))
            n_produced = produce(conf, log, alerts)
            if n_produced != n:
                raise Exception("Failed to produce all alerts in batch: expected {}, got {}".format(n, n_produced))
            c.commit(asynchronous=False)
    except KafkaError as e:
        # TODO handle this properly
        log.warning(str(e))
    finally:
        # if we created our own consumer then close it
        if consumer == None:
            c.close()
    return n


def classify(conf, log, alerts):
    "send a batch of alerts to sherlock and add the responses to the alerts, return the number of alerts classified"
    
    #global alerts

    log.debug('called classify with config: ' + str(conf))
  
    # read Sherlock settings file
    sherlock_settings = {}
    try:
        with open(conf['sherlock_settings'], "r") as f:
            sherlock_settings = yaml.safe_load(f)
    except IOError as e:
        log.error(e)

    # look up objects in cache
    annotations = {}
    if conf['cache_db']:
        names = []
        for alert in alerts:
            name = alert.get('objectId', alert.get('candid'))
            names.append(name)
        query = "SELECT * FROM cache WHERE name IN ('{}');".format("','".join(names))
        url = urlparse(conf['cache_db'])
        connection = pymysql.connect(
                host=url.hostname,
                user=url.username,
                password=url.password,
                db=url.path.lstrip('/'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)        
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                for result in cursor.fetchall():
                    match = json.loads(result.get('crossmatch'))
                    annotations[result['name']] = {
                            'classification': result['class']
                            }
                    for key,value in match.items():
                        annotations[result['name']][key] = value
                    log.debug("Got crossmatch from cache:\n" + json.dumps(match, indent=2))
        except TypeError:
            log.debug("Got TypeError reading cache. Entry probably present, but incomplete or malformed. Ignoring.")
        finally:
            connection.close()
    if len(annotations)>0:
        log.info("got {:d} annotations from cache".format(len(annotations)))

    # make lists of names, ra, dec
    names = []
    ra = []
    dec = []
    for alert in alerts:
        name = alert.get('objectId', alert.get('candid'))
        if not name in annotations:
            if not name in names:
                names.append(name)
                ra.append(alert['candidate']['ra'])
                dec.append(alert['candidate']['dec'])

    # set up sherlock
    classifier = transient_classifier(
        log=log,
        settings=sherlock_settings,
        ra=ra,
        dec=dec,
        name=names,
        verbose=1,
        updateNed=False,
        lite=True
    )

    # run sherlock
    cm_by_name = {}
    if len(names) > 0:
        log.info("running Sherlock classifier on {:d} alerts".format(len(alerts)))
        classifications, crossmatches = classifier.classify()
        log.info("got {:d} classifications".format(len(classifications)))
        log.info("got {:d} crossmatches".format(len(crossmatches)))
        # process classfications
        for name in names:
            if name in classifications:
                annotations[name] = { 'classification': classifications[name][0] }
        # process crossmatches
        for cm in crossmatches:
            name = cm['transient_object_id']
            if name in cm_by_name:
                cm_by_name[name].append(cm)
            else:
                cm_by_name[name] = [cm]
        for name in names:
            if name in cm_by_name:
                cm = cm_by_name[name]
                if len(cm) > 0:
                    match = cm[0]
                    log.debug("got crossmatch:\n {}".format(json.dumps(match, indent=2)))
                    for key, value in match.items():
                        if key != 'rank':
                            annotations[name][key] = value
    else:
        log.info("not running Sherlock as no remaining alerts to process")

    # update cache database
    if conf['cache_db'] and len(names)>0:
        connection = pymysql.connect(
                host=url.hostname,
                user=url.username,
                password=url.password,
                db=url.path.lstrip('/'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)
        values = []
        for name in names:
            classification = annotations[name]['classification']
#            if annotations[name].get('catalogue_object_type'):
#                object_type = "'{}'".format(annotations[name]['catalogue_object_type'])
#            else:
#                object_type = 'NULL'
#            if annotations[name].get('z'):
#                z = "'{:f}'".format(annotations[name]['z'])
#            else:
#                z = 'NULL'
#            if annotations[name].get('separation'):
#                separation = "'{:f}'".format(annotations[name]['separation'])
#            else:
#                separation = 'NULL'
            cm = cm_by_name.get(name, [])
            crossmatch = "'{}'".format(json.dumps(cm[0])) if len(cm) > 0 else "NULL"
            values.append("\n ('{}','{}',{})".format(name, classification, crossmatch))
        #query = "INSERT INTO cache VALUES ('{}','{}',{})".format(name, classification, crossmatch)
        query = "INSERT INTO cache VALUES {}".format(",".join(values))
        log.info("update cache: {}".format(query))
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
        finally:
            connection.commit()
            connection.close()

    # add the annotations to the alerts
    n = 0
    for alert in alerts:
        name = alert.get('objectId', alert.get('candid'))
        annotations[name]['annotator'] = "https://github.com/thespacedoctor/sherlock"
        annotations[name]['additional_output'] = "http://lasair.lsst.ac.uk/api/sherlock/object/" + name
        # placeholders until sherlock returns these
        annotations[name]['description'] = 'Placeholder'
        annotations[name]['summary']  = 'Placeholder'
        if 'annotations' not in alert:
            alert['annotations'] = {}
        alert['annotations']['sherlock'] = []
        alert['annotations']['sherlock'].append(annotations[name])
        n += 1

    return n

def produce(conf, log, alerts):
    "produce a batch of alerts on the kafka output topic, return number of alerts produced"

    #global alerts

    log.debug('called produce with config: ' + str(conf))

    # set up Kafka
    settings = {
        'bootstrap.servers': conf['broker'],
        'session.timeout.ms': 6000
    }
    # TODO add a separate flag for this?
    #if conf['debug']:
    #    settings['debug'] = 'all'
    p = Producer(settings, logger=log)

    # produce alerts
    n = 0
    try:
        while alerts:
            alert = alerts.pop(0)
            p.produce(conf['output_topic'], value=json.dumps(alert))
            n += 1
    #    for name,alert in alerts.items():
    #        p.produce(conf['output_topic'], value=json.dumps(alert))
    #        n += 1
    finally:
        p.flush()
    log.info("produced {:d} alerts".format(n))
    return n

def run(conf, log):
    settings = {
        'bootstrap.servers': conf['broker'],
        'group.id': conf['group'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'enable.auto.commit': False
    }
    consumer = Consumer(settings, logger=log)

    batches = conf['max_batches']
    while batches != 0:
        batches -= 1
        alerts = []
        n = consume(conf, log, alerts, consumer)
#        if n > 0:
#            classify(conf, log, alerts)
#            produce(conf, log, alerts)
#        elif conf['stop_at_end']:
#            break
        if n==0 and conf['stop_at_end']:
            break
         


if __name__ == '__main__':
    # parse cmd line arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-c', '--config', default=None, type=str, help='location of config file')
    parser.add_argument('-b', '--broker', type=str, help='address:port of Kafka broker(s)')
    parser.add_argument('-g', '--group', type=str, default='sherlock-dev-1', help='group id to use for Kafka')
    parser.add_argument('-t', '--timeout', type=int, default=30, help='kafka consumer timeout in s') # 10s is probably a sensible minimum
    parser.add_argument('-e', '--stop_at_end', action='store_true', default=False, help='stop when no more messages to consume')
    parser.add_argument('-i', '--input_topic', type=str, help='name of input topic')
    parser.add_argument('-o', '--output_topic', type=str, help='name of output topic')
    parser.add_argument('-n', '--batch_size', type=int, default=1000, help='number of messages to process per batch')
    parser.add_argument('-l', '--max_batches', type=int, default=-1, help='max number of batches to process')
    parser.add_argument('-m', '--max_errors', type=int, default=-1, help='maximum number of non-fatal errors before aborting') # negative=no limit
    parser.add_argument('-d', '--cache_db', type=str, default='', help='cache database (e.g. mysql://user:pw@host:3306/database)') # empty = don't use cache
    parser.add_argument('-s', '--sherlock_settings', type=str, default='sherlock.yaml', help='location of Sherlock settings file (default sherlock.yaml)')
    parser.add_argument('-q', '--quiet', action="store_true", default=None, help='minimal output')
    parser.add_argument('-v', '--verbose', action="store_true", default=None, help='verbose output')
    parser.add_argument('--debug', action="store_true", default=None, help='debugging output')
    conf = vars(parser.parse_args())

    # use config file if set
    if conf['config']:
        try:
            with open(conf['config'], "r") as f:
                cfg = yaml.safe_load(f)
                for key,value in cfg.items():
                    conf[key] = value
        except IOError as e:
            print (e)

    # set up a logger
    if conf['quiet']:
        logging.basicConfig(level=logging.ERROR)
    elif conf['verbose']:
        logging.basicConfig(level=logging.INFO)
    elif conf['debug']:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    log = logging.getLogger("sherlock_wrapper") 

    # print options on debug
    log.debug("config options:\n"+json.dumps(conf,indent=2))

    # check that required options are set
    if not conf.get('broker'):
        log.error("broker not set")
        sys.exit(2)
    if not conf.get('input_topic'):
        log.error("input topic not set")
        sys.exit(2)
    if not conf.get('output_topic'):
        log.error("output topic not set")
        sys.exit(2)

    run(conf, log)

