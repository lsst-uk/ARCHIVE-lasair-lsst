from unittest import TestCase, mock
import logging
import sys
import json

from sherlock_wrapper import wrapper
from confluent_kafka import KafkaError

log = logging.getLogger()
log.level = logging.ERROR
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)

def non_fatal_error_on_1st_call():
    global called_nfe
    try:
        called_nfe
    except:
        called_nfe = True
        return KafkaError(KafkaError._APPLICATION, "Test Error", fatal=False, retriable=True)
    else:
        return None

class TestConsumer(TestCase):

    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':1,
        'max_errors':-1
        }

    with open("tests/example_ingested.json", 'r') as f:
        example_input_data = f.read()

    # test consumer reaching end of topic
    def test_consume_end_of_topic(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            mock_kafka_consumer.return_value.poll.return_value = None
            wrapper.alerts = {}
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 0)
            # alerts should be empty
            self.assertEqual(wrapper.alerts, {})
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)

             
    # test consuming a batch of alerts
    def test_consume_alert_batch(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = None
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data
            # consume should report consuming 5 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 5)
            # alerts should have len 5
            self.assertEqual(len(wrapper.alerts), 5)
            # content of alerts should be as expected
            # will need rewriting when we fix bug #5
            self.assertEqual(wrapper.alerts.get('ZTF18aapubnx')['candidate']['jd'], 2458943.9334606)
            # poll should have been called 5 times
            self.assertEqual(mock_kafka_consumer.return_value.poll.call_count, 5)

    # test that a fatal error is fatal
    def test_fatal_error(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._FATAL, "Test Error", fatal=True, retriable=False)
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = e
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data
            wrapper.alerts = {}
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 0)
            # alerts should be empty
            self.assertEqual(wrapper.alerts, {})
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)

    # test that a non-fatal error is non-fatal
    def test_non_fatal_error(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            mock_kafka_consumer.return_value.poll.return_value.error = non_fatal_error_on_1st_call
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data
            wrapper.alerts = {}
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 5)
            # alerts should have len 5
            self.assertEqual(len(wrapper.alerts), 5)
            # content of alerts should be as expected
            # will need rewriting when we fix bug #5
            self.assertEqual(wrapper.alerts.get('ZTF18aapubnx')['candidate']['jd'], 2458943.9334606)
            # poll should have been called 6 times
            self.assertEqual(mock_kafka_consumer.return_value.poll.call_count, 6)

    # test max non-fatal errors 
    def test_max_errors(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            mock_kafka_consumer.return_value.poll.return_value.error = non_fatal_error_on_1st_call
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data
            conf = {
                'broker':'',
                'group':'',
                'input_topic':'',
                'output_topic':'',
                'batch_size':5,
                'timeout':1,
                'max_errors':0
                }
            wrapper.alerts = {}
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(conf, log), 0)
            # alerts should be empty
            self.assertEqual(wrapper.alerts, {})
            # poll should have been called once with timeout 1
            mock_kafka_consumer.return_value.poll.assert_called_once_with(1)


class TestClassifier(TestCase):
    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':1,
        'max_errors':-1,
        'sherlock_settings': ''
        }

    with open("tests/example_ingested.json", 'r') as f:
        example_input_data = json.load(f)

    def test_classify_alert_batch(self):
        with mock.patch('sherlock_wrapper.wrapper.transient_classifier') as mock_classifier:
            wrapper.alerts = { "ZTF18aapubnx": self.example_input_data }
            classifications = { "ZTF18aapubnx": "Q" }
            crossmatches = [ { 'transient_object_id':"ZTF18aapubnx", 'thing':'foo' } ]
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 1 alert
            self.assertEqual(wrapper.classify(self.conf, log), 1)
            # length of alerts shouls still be 
            self.assertEqual(len(wrapper.alerts), 1)
            # content of alerts should be as expected
            self.assertEqual(wrapper.alerts['ZTF18aapubnx']['objClass'], 'Q')
            self.assertEqual(wrapper.alerts['ZTF18aapubnx']['matches'], crossmatches)
            # classify should have been called once 
            mock_classifier.return_value.classify.assert_called_once()

class TestProducer(TestCase):
    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':1,
        'max_errors':-1
        }

    # test consuming a batch of alerts
    def test_produce_alert_batch(self):
        with mock.patch('sherlock_wrapper.wrapper.Producer') as mock_kafka_producer:
            wrapper.alerts = { "one": {}, "two": {}, "three": {} }
            # should report producing 3 alerts
            self.assertEqual(wrapper.produce(self.conf, log), 3)
            # alerts should now be empty
            self.assertEqual(wrapper.alerts, {})
            # produce should have been called 3 times
            self.assertEqual(mock_kafka_producer.return_value.produce.call_count, 3)


if __name__ == '__main__':
    unittest.main()
