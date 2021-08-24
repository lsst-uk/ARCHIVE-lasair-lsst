"""Sherlock wrapper integrtation test.

Requires a functional Kafka running on localhost and Sherlock."""

#import unittest, unittest.mock
#import logging
#import sys
#import json

import unittest.main
from unittest import TestCase, expectedFailure
import json
import logging
from time import sleep
import subprocess

import context
from sherlock_wrapper import wrapper

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient


conf = {
        'broker': 'localhost:9092',
        'timeout': 8,
        'group': 'sherlock-test-1',
        'input_topic': 'test_input_2t8sv',
        'output_topic': 'test_output_2t8sv',
        'batch_size': 5,
        'max_batches': 1,
        'max_errors': 1,
        'stop_at_end': False,
        'cache_db': '',
        'sherlock_settings': 'sherlock_test.yaml',
        'poll_timeout': 30,
        'max_poll_interval': 120000
        }

settings = {
        'bootstrap.servers': conf['broker'],
        }

def del_topic(topic):
    admin_client = AdminClient(settings)
    for (topic,future) in admin_client.delete_topics([topic], operation_timeout=4, request_timeout=4).items():
        try:
            future.result(timeout=10)
            sleep(1)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                pass
            else:
                raise e

class IntegrationTest(TestCase):

    @classmethod
    def setUpClass(cls):

        subprocess.Popen(["bin/zookeeper-server-start.sh", "config/zookeeper.properties"], cwd="/opt/kafka")
        subprocess.run(["sudo", "service", "mysql", "start"])        
        sleep(4)
        subprocess.Popen(["bin/kafka-server-start.sh", "config/server.properties"], cwd="/opt/kafka")
        sleep(20)

        with open("example_ingested.json", 'r') as f:
            # load example data
            data = json.load(f)
            # try to delete the topics
            del_topic(conf['input_topic'])
            del_topic(conf['output_topic'])
            # write example messages
            p = Producer(settings)
            try:
                for alert in data:
                    p.produce(conf['input_topic'], value=json.dumps(alert))
            finally:
                p.flush(timeout=20)

    @classmethod
    def tearDownClass(cls):
        del_topic(conf['input_topic'])
        del_topic(conf['output_topic'])
        pass

    # TODO: incorporate cache into test
    def test_1_run_wrapper(self):
        logging.basicConfig(level=logging.WARNING)
        log = logging.getLogger("sherlock_wrapper_test")
        wrapper.run(conf,log)

    def test_2_check_result(self):
        logging.basicConfig(level=logging.WARNING)
        log = logging.getLogger("sherlock_wrapper_test")
        settings = {
            'bootstrap.servers': conf['broker'],
            'group.id': conf['group'],
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        c = Consumer(settings, logger=log)
        c.subscribe([conf['output_topic']])
        n = 0
        n_error = 0
        alerts = []
        try:
            while n < 99:
                # Poll for messages
                msg = c.poll(conf['timeout'])
                if msg is None:
                    # stop when we get to the end of the topic
                    break
                elif not msg.error():
                    alert = json.loads(msg.value())
                    alerts.append(alert)
                    n += 1
                else:
                    n_error += 1
                    log.warning(str(msg.error()))
                    break
        except KafkaError as e:
            log.warning(str(e))
        finally:
            c.close()
        # should be 5 alerts
        self.assertEqual(len(alerts), 5)
        # all alerts shuould have a sherlock classification of ORPHAN
        for alert in alerts:
            self.assertEqual(len(alert['annotations']['sherlock']), 1)
            self.assertEqual(alert['annotations']['sherlock'][0]['classification'], "ORPHAN") 
            self.assertRegexpMatches(alert['annotations']['sherlock'][0]['annotator'], "^https://github.com/thespacedoctor/sherlock")
            self.assertIn('additional_output',alert['annotations']['sherlock'][0])
            # These should not be populated for orphan objects
            self.assertNotIn('catalogue_object_type', alert['annotations']['sherlock'][0])
            self.assertNotIn('z', alert['annotations']['sherlock'][0])

if __name__ == '__main__':
    import xmlrunner
    runner = xmlrunner.XMLTestRunner(output='test-reports')
    unittest.main(testRunner=runner)
    unittest.main()


