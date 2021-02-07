import os, sys
import json
sys.path.append('/home/ubuntu/lasair-lsst/filter')
from insert_query import create_insert_query, create_insert_annotation
from ingestStreamThreaded import sherlock_attributes

datadir = 'sample_alerts'
for filename in os.listdir(datadir):
    tok = filename.split('.')
    uok = tok[0].split('_')
    objectId = uok[0]
    filetype = uok[1]
    if filetype != 'alert':
        continue
    alert = json.loads(open(datadir + '/%s_alert.json' % objectId).read())
    object_query_file   = open(datadir + '/%s_object.json' % objectId).read()
    sherlock_query_file = open(datadir + '/%s_sherlock.json' % objectId).read()
    print(objectId)
    object_query = create_insert_query(alert)

    annClass = 'sherlock'
    ann = alert['annotations'][annClass][0]
    sherlock_query = create_insert_annotation(objectId, annClass, ann, \
        sherlock_attributes, 'sherlock_classifications', replace=True)
