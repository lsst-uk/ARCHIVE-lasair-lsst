""" Sample code for user-created annotation of Lasair alerts.
-- First we make a Lasair client (L) that can be used to run the API. In Particular, we wll get the lightcurve for any given objectId.
-- Next is the consumer (C) that gets a stream from Lasair, associated with some pre-query. Below we use 'SN-likecandidates' as the query/stream to be consumed.
-- Finally we have the producer (P) which is used to push back to Lasair the results of a calculation based on the lightcurve -- and any other attributes reported by Lasair.
"""

import json, sys
from lasair import lasair_client, lasair_consumer, lasair_producer
import random
import settings

try:
    nannotation = int(sys.argv[1])
except:
    nannotation = 10
print('Trying %d annotations' % nannotation)

# The API token for the user who is running this
token = settings.API_TOKEN
L = lasair_client(token)

# The pre-query we are using to get objects that may be interesting
topic_in = 'lasair_2SN-likecandidates'

# Group_id controls if we start from where we left off, or get all the alerts from the beginning of the kafka cache
group_id = 'test123'
print('Using topic=', topic_in, 'group_id=', group_id)

# Make kafka consumer
C = lasair_consumer(settings.KAFKA_CONSUME, group_id, topic_in)

# Make kafka producer. Each annotator gets the username, password, and outgoing topic from Lasair staff
P = lasair_producer(settings.KAFKA_PRODUCE, settings.KAFKA_USERNAME, settings.KAFKA_PASSWORD, settings.KAFKA_OUT)

# Get 100 messages from the pre-query and send back a trivial annotation for each one
for i in range(nannotation):
    msg = C.poll()
    if msg is None:
        break
    if msg.error():
        print(str(msg.error()))
        break

    # messages coem as JSON encoded
    jmsg = json.loads(msg.value())

    # get the objectId
    objectId = jmsg['objectId']

    # get all the info about the object
    objectData = L.objects([objectId])[0]

    # list of all the candidate, i.e. the lightcurve
    ncand = len(objectData['candidates'])

    # trivial annotation!
    if ncand < 20: 
        classification = 'short'
        explanation     = 'not too many'
    elif ncand < 100:
        classification = 'medium'
        explanation     = 'quite a few'
    else:
        classification = 'long'
        explanation     = 'lots of detections'

    version = None
    classdict = {'ncand': ncand}
    url = None

    if random.random() < 0.1:
        classification = None
    if random.random() < 0.1:
        version = '0.3'
    if random.random() < 0.1:
        explanation = None
    if random.random() < 0.1:
        classdict = []
    if random.random() < 0.1:
        classdict = 42
    if random.random() < 0.1:
        url = 'http://cnn.com'

    print(objectId, classification, version, explanation, classdict, url)

    # sent it back to Lasair to be put in the database
    try:
        P.produce(objectId, 
            classification, 
            version=version, 
            explanation=explanation, 
            classdict=classdict,
            url=url)
    except Exception as e:
        print(e)

# dont forget to flush or all your efforts will be for nothing
P.flush()
