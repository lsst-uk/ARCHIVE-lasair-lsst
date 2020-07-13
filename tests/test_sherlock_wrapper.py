import unittest, unittest.mock
import logging
import sys
import json

import context
from sherlock_wrapper import wrapper
from confluent_kafka import KafkaError

log = logging.getLogger()
log.level = logging.ERROR
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)

with open("example_ingested.json", 'r') as f:
    data = json.load(f)
    example_alert = data[0]
    example_input_data = json.dumps(data[0])

# mock kafka message class for use by mock poll
class MockMessage:
    def __init__(self,err=None):
        self.err = err
    def value(self):
        #with open("tests/example_ingested.json", 'r') as f:
        #    example_input_data = f.read()[0]
        return example_input_data
    def error(self):
        return self.err
    def offset(self):
        return 0

# mock poll method that returns an error the 1st time called
nfe_call_count = 0
def non_fatal_error_on_1st_call(timeout):
    global nfe_call_count
    nfe_call_count += 1
    if nfe_call_count > 1:
        return MockMessage()
    else:
        e = KafkaError(KafkaError._APPLICATION, "Test Error", fatal=False, retriable=True)
        return MockMessage(e)

class TestConsumer(unittest.TestCase):

    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':1,
        'max_errors':-1,
        'cache_db':''
        }

    #with open("tests/example_ingested.json", 'r') as f:
    #    example_input_data = f.read()[0]

    # test consumer reaching end of topic
    def test_consume_end_of_topic(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            mock_kafka_consumer.return_value.poll.return_value = None
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log, alerts), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)

             
    # test consuming a batch of alerts
    def test_consume_alert_batch(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.classify') as mock_classify:
            with unittest.mock.patch('sherlock_wrapper.wrapper.produce') as mock_produce:
                with unittest.mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
                    mock_classify.return_value = 5
                    mock_produce.return_value = 5
                    mock_kafka_consumer.return_value.poll.return_value.error.return_value = None
                    mock_kafka_consumer.return_value.poll.return_value.value.return_value = example_input_data
                    alerts = []
                    # consume should report consuming 5 alerts
                    self.assertEqual(wrapper.consume(self.conf, log, alerts), 5)
                    # alerts should have len 5
                    self.assertEqual(len(alerts), 5)
                    # content of alerts should be as expected
                    self.assertEqual(alerts[0]['candidate']['jd'], 2458943.9334606)
                    # poll should have been called 5 times
                    self.assertEqual(mock_kafka_consumer.return_value.poll.call_count, 5)

    # test that a fatal error is fatal
    def test_fatal_error(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._FATAL, "Test Error", fatal=True, retriable=False)
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = e
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = example_input_data
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log, alerts), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)

    # test that a non-fatal error is non-fatal
    def test_non_fatal_error(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.classify') as mock_classify:
            with unittest.mock.patch('sherlock_wrapper.wrapper.produce') as mock_produce:
                with unittest.mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
                    mock_classify.return_value = 5
                    mock_produce.return_value = 5
                    # poll returns None when no messages left to consume
                    mock_kafka_consumer.return_value.poll = non_fatal_error_on_1st_call
                    alerts = []
                    # consume should report consuming 0 alerts
                    self.assertEqual(wrapper.consume(self.conf, log, alerts), 5)
                    # alerts should have len 5
                    self.assertEqual(len(alerts), 5)
                    # content of alerts should be as expected
                    self.assertEqual(alerts[0]['candidate']['jd'], 2458943.9334606)
                    # poll should have been called 6 times
                    self.assertEqual(nfe_call_count, 6)

    # test max non-fatal errors 
    def test_max_errors(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._APPLICATION, "Test Error", fatal=False, retriable=True)
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = e
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = example_input_data
            conf = {
                'broker':'',
                'group':'',
                'input_topic':'',
                'output_topic':'',
                'batch_size':5,
                'timeout':1,
                'max_errors':0,
                'cache_db':''
                }
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(conf, log, alerts), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)


class TestClassifier(unittest.TestCase):
    def test_classify_alert_batch(self):
        conf = {
            'broker':'',
            'group':'',
            'input_topic':'',
            'output_topic':'',
            'batch_size':5,
            'timeout':1,
            'max_errors':-1,
            'cache_db':'',
            'sherlock_settings': 'sherlock_test.yaml'
            }
        with unittest.mock.patch('sherlock_wrapper.wrapper.transient_classifier') as mock_classifier:
            alerts = [ example_alert.copy() ]
            classifications = { "ZTF18aapubnx": "Q" }
            crossmatches = [ { 'transient_object_id':"ZTF18aapubnx", 'thing':'foo' } ]
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 1 alert
            self.assertEqual(wrapper.classify(conf, log, alerts), 1)
            # length of alerts shouls still be 1
            self.assertEqual(len(alerts), 1)
            # content of alerts should be as expected
            self.assertEqual(alerts[0]['sherlock_classification'], 'Q')
            self.assertEqual(alerts[0]['matches'], crossmatches)
            # classify should have been called once 
            mock_classifier.return_value.classify.assert_called_once()

    def test_classify_cache_hit(self):
        conf = {
            'broker':'',
            'group':'',
            'input_topic':'',
            'output_topic':'',
            'batch_size':5,
            'timeout':1,
            'max_errors':-1,
            'cache_db':'mysql://user_name:password@localhost:3306/sherlock_cache',
            'sherlock_settings': 'sherlock_test.yaml'
            }
        with unittest.mock.patch('sherlock_wrapper.wrapper.transient_classifier') as mock_classifier:
            with unittest.mock.patch('sherlock_wrapper.wrapper.pymysql.connect') as mock_pymysql:
                alerts = [ example_alert.copy() ]
                classifications = { "ZTF18aapubnx": "Q" }
                crossmatches = [ { 'transient_object_id':"ZTF18aapubnx", 'thing':'foo' } ]
                mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = [{'name': 'ZTF18aapubnx', 'class': 'T'}]
                # should report classifying 1 alert
                self.assertEqual(wrapper.classify(conf, log, alerts), 1)
                # length of alerts shouls still be 1
                self.assertEqual(len(alerts), 1)
                # content of alerts should be as expected - from cache
                self.assertEqual(alerts[0]['sherlock_classification'], 'T')
                # if we got a cache hit then we don't get crossmatches
                self.assertNotIn('matches', alerts[0])
                # classify should not have been called
                mock_classifier.return_value.classify.assert_not_called()

    def test_classify_cache_miss(self):
        conf = {
            'broker':'',
            'group':'',
            'input_topic':'',
            'output_topic':'',
            'batch_size':5,
            'timeout':1,
            'max_errors':-1,
            'cache_db':'mysql://user_name:password@localhost:3306/sherlock_cache',
            'sherlock_settings': 'sherlock_test.yaml'
            }
        with unittest.mock.patch('sherlock_wrapper.wrapper.transient_classifier') as mock_classifier:
            with unittest.mock.patch('sherlock_wrapper.wrapper.pymysql.connect') as mock_pymysql:
                alerts = [ example_alert.copy() ]
                classifications = { "ZTF18aapubnx": "Q" }
                crossmatches = [ { 'transient_object_id':"ZTF18aapubnx", 'thing':'foo' } ]
                mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = []
                # should report classifying 1 alert
                self.assertEqual(wrapper.classify(conf, log, alerts), 1)
                # length of alerts shouls still be 1
                self.assertEqual(len(alerts), 1)
                # content of alerts should be as expected - from sherlock
                self.assertEqual(alerts[0]['sherlock_classification'], 'Q')
                self.assertEqual(alerts[0]['matches'], crossmatches)
                # classify should have been called once
                mock_classifier.return_value.classify.assert_called_once()
                # execute should have been called twice
                self.assertEqual(mock_pymysql.return_value.cursor.return_value.__enter__.return_value.execute.call_count, 2)
                # fetchall should have been called once
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.assert_called_once()

class TestProducer(unittest.TestCase):
    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':1,
        'max_errors':-1,
        'cache_db':''
        }

    # test producing a batch of alerts
    def test_produce_alert_batch(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.Producer') as mock_kafka_producer:
            alerts = [ {}, {}, {} ]
            self.assertEqual(len(alerts), 3)
            # should report producing 3 alerts
            self.assertEqual(wrapper.produce(self.conf, log, alerts), 3)
            # alerts should now be empty
            self.assertEqual(alerts, [])
            self.assertEqual(len(alerts), 0)
            # produce should have been called 3 times
            self.assertEqual(mock_kafka_producer.return_value.produce.call_count, 3)


if __name__ == '__main__':
    import xmlrunner 
    runner = xmlrunner.XMLTestRunner(output='test-reports')
    unittest.main(testRunner=runner)
    unittest.main()
