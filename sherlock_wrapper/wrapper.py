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
from confluent_kafka import Consumer, Producer, KafkaError
from mock_sherlock import transient_classifier
#from sherlock import transient_classifier

# TODO replace with a proper queue(s) for multi-threading?
alerts = {}

def consume(conf, log):
    "fetch a batch of alerts from kafka, return number of alerts consumed"

    global alerts

    log.debug('called consume with config: ' + str(conf))
    
    # Kafka settings
    settings = {
        'bootstrap.servers': conf['broker'],
        'group.id': conf['group'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }
    # TODO add a separate flag for this?
    #if conf['debug']:
    #    settings['debug'] = 'all'

    #max_messages = conf.get('max_messages', float('inf'))

    c = Consumer(settings, logger=log)
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
                name = alert.get('objectId', alert.get('candid'))
                alerts[name] = alert
            else:
                n_error += 1
                log.warning(str(msg))
                try:
                    if msg.error().fatal:
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
            n += 1
    except KafkaError as e:
        # TODO handle this properly
        log.warning(str(e))
    finally:
        c.close()
    log.info("consumed {:d} alerts".format(n))
    return n


def classify(conf, log):
    "send a batch of alerts to sherlock and add the responses to the alerts, return the number of alerts classified"
    
    global alerts

    log.debug('called classify with config: ' + str(conf))
    
    # make lists of names, ra, dec
    names = []
    ra = []
    dec = []
    for name,alert in alerts.items():
        names.append(name)
        ra.append(alert['candidate']['ra'])
        dec.append(alert['candidate']['dec'])

    # set up sherlock
    classifier = transient_classifier(
        log=log,
        settings=conf['sherlock_settings'],
        ra=ra,
        dec=dec,
        name=names,
        verbose=1,
        updateNed=False
    )

    # run sherlock
    log.info("running Sherlock classifier on {:d} alerts".format(len(alerts)))
    classifications, crossmatches = classifier.classify()
    log.info("got {:d} classifications".format(len(classifications)))
    log.info("got {:d} crossmatches".format(len(crossmatches)))
    
    # process classifications
    for name,classes in classifications.items():
        alerts[name]['objClass'] = classes[0]

    # process crossmatches
    for cm in crossmatches:
        name = cm['transient_object_id']
        if 'matches' in alerts[name]:
            alerts[name]['matches'].append(cm)
        else:
            alerts[name]['matches'] = [cm]

    return len(classifications)

def produce(conf, log):
    "produce a batch of alerts on the kafka output topic, return number of alerts produced"

    global alerts

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
        for name,alert in alerts.items():
            p.produce(conf['output_topic'], value=json.dumps(alert))
            n += 1
    finally:
        p.flush()
    log.info("produced {:d} alerts".format(n))
    # clear alerts
    alerts = {}
    return n

if __name__ == '__main__':
    # parse cmd line arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-c', '--config', type=str, default='config.yaml', help='location of config file (default config.yaml)')
    parser.add_argument('-b', '--broker', type=str, help='address:port of Kafka broker(s)')
    parser.add_argument('-g', '--group', type=str, default='sherlock-dev-1', help='group id to use for Kafka')
    parser.add_argument('-t', '--timeout', type=int, default=10, help='kafka consumer timeout in s')
    parser.add_argument('-i', '--input_topic', type=str, help='name of input topic')
    parser.add_argument('-o', '--output_topic', type=str, help='name of output topic')
    parser.add_argument('-n', '--batch_size', type=int, default=1000, help='number of messages to process per batch')
    parser.add_argument('-e', '--max_errors', type=int, default=-1, help='maximum number of non-fatal errors before aborting') # negative=no limit
    parser.add_argument('-s', '--sherlock_settings', type=str, default='sherlock.yaml', help='location of Sherlock settings file (default sherlock.yaml)')
    parser.add_argument('-q', '--quiet', action="store_true", default=None, help='minimal output')
    parser.add_argument('-v', '--verbose', action="store_true", default=None, help='verbose output')
    parser.add_argument('--debug', action="store_true", default=None, help='debugging output')
    conf = vars(parser.parse_args())

    # use config file for arguments not set on cmd line
    try:
        with open(conf['config'], "r") as f:
            cfg = yaml.safe_load(f)
            for key,value in cfg.items():
                if (key not in conf) or (conf[key] is None):
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

    while True:
        consume(conf, log)
        if len(alerts) > 0:
            classify(conf, log)
            produce(conf, log)

