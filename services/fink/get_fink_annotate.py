import os, sys
import random
from datetime import datetime
sys.path.append('/home/ubuntu/lasair-lsst/services/fink/fink-client')
from fink_client.consumer import AlertConsumer
import fink_client.fink_client_conf as fcc
import lasair
import settings

group_id = 'test12'
topic_out = 'fink_early_sn'

# Lasair client
L = lasair.lasair_client(settings.API_TOKEN)

# Fink configuration
fink_config = {
    'username': fcc.username,
    'bootstrap.servers': fcc.servers,
    'group_id': group_id
}

# Instantiate a consumer
consumer = AlertConsumer(fcc.mytopics, fink_config)

nalert = 0
maxtimeout = 5
while 1:
    (topic, alert, version) = consumer.poll(maxtimeout)
    if topic is None:
        break

    objectId = alert['objectId']
    classification = 'early_sn'
    classdict = {
            'rfcsore':           float(alert['rfscore']), 
            'snn_snia_vs_nonia': float(alert['snn_snia_vs_nonia']), 
            'snn_sn_vs_all':     float(alert['snn_sn_vs_all']), 
            'knscore':           float(alert['knscore']),
    }

    L.annotate(
        topic_out,
        objectId,
        classification,
        version='0.1',
        explanation='',
        classdict=classdict,
        url='')

    nalert += 1
if nalert > 0:
    print('\n-- %d from Fink at %s' % (nalert, datetime.now()))
