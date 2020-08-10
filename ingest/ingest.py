import os,sys
import time
from socket import gethostname
from datetime import datetime
import settings
import date_nid
""" Ingest a kafka stream from ZTF
   Usage: python ingest.py       ... means today's stream
   Usage: python ingest.py <nid> ... means a previous night
   Usage: python ingest.py test  ... means the test stream
   """

nid = 0
if len(sys.argv) > 1:
    if sys.argv[1] == 'test':
        topic = 'ztf_test'
    else:
        nid = int(sys.argv[1])
else:
    nid  = date_nid.nid_now()

if nid > 0:
    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'

os.system('date')

print('INGEST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
print('ingest from kafka')
print("Topic is %s, nid is %d" % (topic, nid))
t = time.time()

cmd =  'python3 ingestBatch.py '
cmd += '--maxalert %d ' % settings.KAFKA_MAXALERTS
cmd += '--nthread %d '  % settings.KAFKA_THREADS
cmd += '--stampdir %s ' % settings.BLOB_FILEROOT
cmd += '--group %s '    % settings.KAFKA_GROUPID
cmd += '--topic %s '    % topic
cmd += '--topicout %s ' % settings.KAFKA_TOPIC_OUT
#cmd += '--topicout %s_ingested ' % topic

print(cmd)
rc = os.system(cmd)
print('INGEST duration %.1f seconds returns %d' % ((time.time() - t), rc))

if rc > 0: sys.exit(1)
else:      sys.exit(0)
