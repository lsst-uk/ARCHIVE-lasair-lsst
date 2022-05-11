""" Ingest avro files from a directory and convert to a kafka topic
    up to a maximum maxalert
"""
from confluent_kafka import Producer, KafkaError
import os, sys

if len(sys.argv) < 3:
    print('Usage: files2kafka.py directory topic <maxalert>')
    sys.exit()

directory = sys.argv[1]
topic     = sys.argv[2]

maxalert = -1
if len(sys.argv) > 3:
    maxalert = int(sys.argv[3])

# will need to change this as appropriate
kafka_service = '192.41.108.22:9092'

conf = {
    'bootstrap.servers': kafka_service,
    'client.id': 'client-1',
}

p = Producer(conf)

filenames = sorted(os.listdir(directory))

nalert = 0
for filename in filenames:
    message = open(directory +'/'+ filename, 'rb').read()
    p.produce(topic, message)
    nalert += 1
    if maxalert > 0 and nalert >= maxalert:
        break
    if nalert%1000 == 0:
        print(nalert)
        p.flush()
p.flush()
print('%d alerts ingested from directory %s to topic %s' % (nalert, directory, topic))
