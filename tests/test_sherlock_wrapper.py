from unittest import TestCase, mock
import logging

from sherlock_wrapper import wrapper

class TestConsumer(TestCase):

    conf = {
        'broker':'',
        'group':'',
        'input_topic':'',
        'output_topic':'',
        'batch_size':5,
        'timeout':'1'
    }

    with open("tests/example_ingested.json", 'r') as f:
        example_input_data = f.read()

    # test consumer reaching end of topic
    def test_consume_end_of_topic(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            mock_kafka_consumer.return_value.poll.return_value = None
            logging.basicConfig(level=logging.ERROR)
            log = logging.getLogger()

            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 0)
             
    # test consuming a batch of alerts
    def test_consume_alert_batch(self):
        with mock.patch('sherlock_wrapper.wrapper.Consumer') as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            mock_kafka_consumer.return_value.poll.return_value.error.return_value = None
            mock_kafka_consumer.return_value.poll.return_value.value.return_value = self.example_input_data
            logging.basicConfig(level=logging.ERROR)
            log = logging.getLogger()

            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log), 5)

class TestClassifier(TestCase):
    def test_classify(self):
        pass

class TestProducer(TestCase):
    def test_produce(self):
        pass

if __name__ == '__main__':
    unittest.main()
