import sys
import json
from confluent_kafka import Consumer, KafkaError
# Toy code to read back the ingested test stream and extract objectId, ra, dec

conf = {
    'bootstrap.servers': '192.41.108.22:9092',
    'group.id': 'LASAIR5',
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
streamReader = Consumer(conf)
topic = 'ztf_test_ingested'
streamReader.subscribe([topic])
while 1:
    msg = streamReader.poll(timeout=20)
    if msg == None: break
    s = json.loads(msg.value())
    print('%s, %.6f, %.6f' % (s['objectId'], s['candidate']['ra'], s['candidate']['dec']))

streamReader.close()

