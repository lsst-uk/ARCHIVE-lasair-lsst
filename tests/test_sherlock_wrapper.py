from unittest import TestCase, mock
import logging
import sys

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

            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 0)
             
    # test consuming a batch of alerts
    def test_consume_alert_batch(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = None
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data

            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 5)

    # test that a fatal error is fatal
    def test_fatal_error(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._FATAL, "Test Error", fatal=True, retriable=False)
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = e
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data

            # consume should report consuming 2 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 0)


    # test that a non-fatal error is non-fatal
    def test_non_fatal_error(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            mock_kafka_consumer.return_value.poll.return_value.error = non_fatal_error_on_1st_call
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data

            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 5)

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
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(conf, log), 0)


class TestClassifier(TestCase):
    def test_classify(self):
        pass

class TestProducer(TestCase):
    def test_produce(self):
        pass

if __name__ == '__main__':
    unittest.main()
