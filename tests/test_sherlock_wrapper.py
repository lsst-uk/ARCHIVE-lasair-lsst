import unittest, unittest.mock
import logging
import sys
import json

import context
from sherlock_wrapper import wrapper
from confluent_kafka import KafkaError, KafkaException

log = logging.getLogger()
log.level = logging.ERROR
#log.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)

with open("example_ingested.json", 'r') as f:
    data = json.load(f)
    example_alert = data[0]
    example_input_data = json.dumps(data[0])

with open("example_ingested_ss.json", 'r') as f:
    data = json.load(f)
    example_alert_ss = data[0]
    example_alert_no_ss = data[1]
    example_input_ss_data = json.dumps(data[0])

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
        'cache_db':'',
        'poll_timeout':1,
        'max_poll_interval':300000
        }

    #with open("tests/example_ingested.json", 'r') as f:
    #    example_input_data = f.read()[0]

    # test consumer reaching end of topic
    def test_consume_end_of_topic(self):
        with unittest.mock.MagicMock() as mock_kafka_consumer:
            mock_kafka_consumer.poll.return_value = None
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log, alerts, mock_kafka_consumer), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.poll.assert_called_once_with(1)

             
    # test consuming a batch of alerts
    def test_consume_alert_batch(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.classify') as mock_classify:
            with unittest.mock.patch('sherlock_wrapper.wrapper.produce') as mock_produce:
                with unittest.mock.MagicMock() as mock_kafka_consumer:
                    mock_classify.return_value = 5
                    mock_produce.return_value = 5
                    mock_kafka_consumer.poll.return_value.error.return_value = None
                    mock_kafka_consumer.poll.return_value.value.return_value = example_input_data
                    alerts = []
                    # consume should report consuming 5 alerts
                    self.assertEqual(wrapper.consume(self.conf, log, alerts, mock_kafka_consumer), 5)
                    # alerts should have len 5
                    self.assertEqual(len(alerts), 5)
                    # content of alerts should be as expected
                    self.assertEqual(alerts[0]['candidate']['jd'], 2458943.9334606)
                    # poll should have been called 5 times
                    self.assertEqual(mock_kafka_consumer.poll.call_count, 5)

    # test that a fatal error is fatal
    def test_fatal_error(self):
        with unittest.mock.MagicMock() as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._FATAL, "Test Error", fatal=True, retriable=False)
            mock_kafka_consumer.poll.return_value.error.return_value = e
            mock_kafka_consumer.poll.return_value.value.return_value = example_input_data
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(self.conf, log, alerts, mock_kafka_consumer), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.poll.assert_called_once_with(1)

    # test that a non-fatal error is non-fatal
    def test_non_fatal_error(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.classify') as mock_classify:
            with unittest.mock.patch('sherlock_wrapper.wrapper.produce') as mock_produce:
                with unittest.mock.MagicMock() as mock_kafka_consumer:
                    mock_classify.return_value = 5
                    mock_produce.return_value = 5
                    # poll returns None when no messages left to consume
                    mock_kafka_consumer.poll = non_fatal_error_on_1st_call
                    alerts = []
                    # consume should report consuming 0 alerts
                    self.assertEqual(wrapper.consume(self.conf, log, alerts, mock_kafka_consumer), 5)
                    # alerts should have len 5
                    self.assertEqual(len(alerts), 5)
                    # content of alerts should be as expected
                    self.assertEqual(alerts[0]['candidate']['jd'], 2458943.9334606)
                    # poll should have been called 6 times
                    self.assertEqual(nfe_call_count, 6)

    # test max non-fatal errors 
    def test_max_errors(self):
        with unittest.mock.MagicMock() as mock_kafka_consumer:
            # poll returns None when no messages left to consume
            e = KafkaError(KafkaError._APPLICATION, "Test Error", fatal=False, retriable=True)
            mock_kafka_consumer.poll.return_value.error.return_value = e
            mock_kafka_consumer.poll.return_value.value.return_value = example_input_data
            conf = {
                'broker':'',
                'group':'',
                'input_topic':'',
                'output_topic':'',
                'batch_size':5,
                'timeout':1,
                'max_errors':0,
                'cache_db':'',
                'poll_timeout':1,
                'max_poll_interval':300000
                }
            alerts = []
            # consume should report consuming 0 alerts
            self.assertEqual(wrapper.consume(conf, log, alerts, mock_kafka_consumer), 0)
            # alerts should be empty
            self.assertEqual(alerts, [])
            # poll should have been called once with timeout 1
            mock_kafka_consumer.poll.assert_called_once_with(1)

    # test (non-fatal) failure on commit
    def test_failed_commit(self):
        with unittest.mock.patch('sherlock_wrapper.wrapper.classify') as mock_classify:
            with unittest.mock.patch('sherlock_wrapper.wrapper.produce') as mock_produce:
                with unittest.mock.MagicMock() as mock_kafka_consumer:
                    mock_classify.return_value = 5
                    mock_produce.return_value = 5
                    mock_kafka_consumer.poll.return_value.error.return_value = None
                    mock_kafka_consumer.poll.return_value.value.return_value = example_input_data
                    e = KafkaError(KafkaError._NO_OFFSET, 'test no offset', fatal=False)
                    mock_kafka_consumer.commit.side_effect = KafkaException(e)
                    alerts = []
                    # consume should report consuming 5 alerts
                    self.assertEqual(wrapper.consume(self.conf, log, alerts, mock_kafka_consumer), 5)
                    # alerts should have len 5
                    self.assertEqual(len(alerts), 5)
                    # content of alerts should be as expected
                    self.assertEqual(alerts[0]['candidate']['jd'], 2458943.9334606)

class TestClassifier(unittest.TestCase):
    crossmatches = [ {
                    'rank':1,
                    'transient_object_id':"ZTF18aapubnx",
                    'association_type':'SN',
                    'catalogue_table_name':'NED D/SDSS',
                    'catalogue_object_id':'NGC0716',
                    'catalogue_object_type':'galaxy',
                    'raDeg':28.248701,
                    'decDeg':12.708479,
                    'separationArcsec':31.06,
                    'northSeparationArcsec':-16.204,
                    'eastSeparationArcsec':-26.493,
                    'physical_separation_kpc':8.52,
                    'direct_distance':56.6,
                    'distance':65.88,
                    'z':0.02,
                    'photoZ':0.02,
                    'photoZErr':0.01,
                    'Mag':17.41,
                    'MagFilter':'r',
                    'MagErr':0.0,
                    'classificationReliability':2} ]

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
            #crossmatches = [ { 'transient_object_id':"ZTF18aapubnx", 'rank':1, 'z':1.2, 'catalogue_object_type':'thing', 'separationArcsec':0.1} ]
            crossmatches = TestClassifier.crossmatches
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 1 alert
            self.assertEqual(wrapper.classify(conf, log, alerts), 1)
            # length of alerts shouls still be 1
            self.assertEqual(len(alerts), 1)
            # content of alerts should be as expected
            self.assertRegexpMatches(alerts[0]['annotations']['sherlock'][0]['annotator'], "^https://github.com/thespacedoctor/sherlock")
            self.assertEqual(alerts[0]['annotations']['sherlock'][0]['additional_output'], "http://lasair.lsst.ac.uk/api/sherlock/object/ZTF18aapubnx")
            self.assertEqual(alerts[0]['annotations']['sherlock'][0]['classification'], 'Q')
            for key, value in crossmatches[0].items():
                if key != 'rank':
                    self.assertEqual(alerts[0]['annotations']['sherlock'][0][key], value)
            # classify should have been called once 
            mock_classifier.return_value.classify.assert_called_once()

    # check that a missing ssnamenr field is treated as null
    def test_classify_no_ss_alert(self):
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
            alerts = [ example_alert_no_ss.copy() ]
            classifications = { "ZTF18aapubnx": "Q" }
            crossmatches = TestClassifier.crossmatches
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 1 alert
            self.assertEqual(wrapper.classify(conf, log, alerts), 1)
            # length of alerts shouls still be 1
            self.assertEqual(len(alerts), 1)
            # content of alerts should be as expected
            self.assertRegexpMatches(alerts[0]['annotations']['sherlock'][0]['annotator'], "^https://github.com/thespacedoctor/sherlock")
            self.assertEqual(alerts[0]['annotations']['sherlock'][0]['additional_output'], "http://lasair.lsst.ac.uk/api/sherlock/object/ZTF18aapubnx")
            self.assertEqual(alerts[0]['annotations']['sherlock'][0]['classification'], 'Q')
            for key, value in crossmatches[0].items():
                if key != 'rank':
                    self.assertEqual(alerts[0]['annotations']['sherlock'][0][key], value)
            # classify should have been called once
            mock_classifier.return_value.classify.assert_called_once()

    def test_classify_ss_alert(self):
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
            alerts = [ example_alert_ss.copy() ]
            classifications = { "ZTF18aapubnx": "Q" }
            crossmatches = TestClassifier.crossmatches
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 0 alerts
            self.assertEqual(wrapper.classify(conf, log, alerts), 0)
            # length of alerts shouls still be 1
            self.assertEqual(len(alerts), 1)
            # alert should not have been classified
            self.assertNotIn('annotations', alerts[0])
            # classify should not have been called 
            mock_classifier.return_value.classify.assert_not_called()
   
    def test_classify_description(self):
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
            classifications = { "ZTF18aapubnx": ["Q", "Descr"] }
            crossmatches = TestClassifier.crossmatches
            mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
            # should report classifying 1 alert
            self.assertEqual(wrapper.classify(conf, log, alerts), 1)
            # content of alerts should be as expected
            self.assertEqual(alerts[0]['annotations']['sherlock'][0]['description'], 'Descr')

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
                cache = [{'name': 'ZTF18aapubnx', 'class': 'T', 'crossmatch': json.dumps(TestClassifier.crossmatches[0])}]
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = cache
                # should report classifying 1 alert
                self.assertEqual(wrapper.classify(conf, log, alerts), 1)
                # length of alerts shouls still be 1
                self.assertEqual(len(alerts), 1)
                # content of alerts should be as expected - from cache
                self.assertEqual(alerts[0]['annotations']['sherlock'][0]['classification'], 'T')
                self.assertRegexpMatches(alerts[0]['annotations']['sherlock'][0]['annotator'], "^https://github.com/thespacedoctor/sherlock")
                self.assertEqual(alerts[0]['annotations']['sherlock'][0]['additional_output'], "http://lasair.lsst.ac.uk/api/sherlock/object/ZTF18aapubnx")
                for key, value in TestClassifier.crossmatches[0].items():
                    if key != 'rank':
                        self.assertEqual(alerts[0]['annotations']['sherlock'][0][key], value)
                # classify should not have been called
                mock_classifier.return_value.classify.assert_not_called()

    # if we get a cache hit but the crossmatch is empty/malformed then we should ignore it
    def test_classify_cache_empty_hit(self):
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
                cache = [{'name': 'ZTF18aapubnx', 'class': 'T', 'crossmatch': None}]
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = cache
                # should report classifying 1 alert
                self.assertEqual(wrapper.classify(conf, log, alerts), 1)
                # content of alerts should be from sherlock - cache should be ignored
                self.assertEqual(alerts[0]['annotations']['sherlock'][0]['classification'], 'Q')
                # classify *should* have been called
                mock_classifier.return_value.classify.assert_called_once()

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
                crossmatches = TestClassifier.crossmatches
                mock_classifier.return_value.classify.return_value = (classifications, crossmatches)
                mock_pymysql.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = []
                # should report classifying 1 alert
                self.assertEqual(wrapper.classify(conf, log, alerts), 1)
                # length of alerts shouls still be 1
                self.assertEqual(len(alerts), 1)
                # content of alerts should be as expected - from sherlock
                self.assertEqual(alerts[0]['annotations']['sherlock'][0]['classification'], 'Q')
                self.assertRegexpMatches(alerts[0]['annotations']['sherlock'][0]['annotator'], "^https://github.com/thespacedoctor/sherlock")
                self.assertEqual(alerts[0]['annotations']['sherlock'][0]['additional_output'], "http://lasair.lsst.ac.uk/api/sherlock/object/ZTF18aapubnx")
                for key, value in TestClassifier.crossmatches[0].items():
                    if key != 'rank':
                        self.assertEqual(alerts[0]['annotations']['sherlock'][0][key], value)
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
