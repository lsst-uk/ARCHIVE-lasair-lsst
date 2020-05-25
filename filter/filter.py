import os,sys
import time
from socket import gethostname
from datetime import datetime
import settings
import date_nid

if len(sys.argv) > 1:
    nid = int(sys.argv[1])
else:
    nid  = date_nid.nid_now()

date = date_nid.nid_to_date(nid)
topic  = 'ztf_' + date + '_programid1'

os.system('date')
print('clear local caches')
cmd = 'python3 refresh.py'
os.system(cmd)

print('INGEST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
print('ingest from kafka')
print("Topic is %s, nid is %d" % (topic, nid))
t = time.time()

cmd =  'python3 ingestStreamThreaded.py '
cmd += '--maxalert %d ' % settings.KAFKA_MAXALERTS
cmd += '--nthread %d '  % settings.KAFKA_THREADS
cmd += '--group %s '    % settings.KAFKA_GROUPID
cmd += '--host %s '     % settings.KAFKA_PRODUCER
cmd += '--topic ' + topic

print(cmd)
os.system(cmd)
print('INGEST duration %.1f seconds' % (time.time() - t))

t = time.time()
print('SEND to ARCHIVE')
cmd = 'mysql --user=ztf --database=ztf --password=%s < output_csv.sql' % settings.DB_PASS_WRITE
os.system(cmd)

outfile = '/home/ubuntu/scratch/out.txt'
cmd = 'mv /var/lib/mysql-files/out.txt %s' % outfile
os.system(cmd)

if os.path.exists(outfile) and os.stat(outfile).st_size == 0:
    print('SEND outfile is empty')
    print('SEND %.1f seconds' % (time.time() - t))
    sys.exit(1)

out = gethostname()

cmd = 'scp /home/ubuntu/scratch/out.txt %s:scratch/%s' % (settings.DB_HOST_REMOTE, out)
os.system(cmd)

cmd = 'ssh %s "python3 /home/ubuntu/lasair-lsst/lasair-db/archive_in.py %s"' % (settings.DB_HOST_REMOTE, out)
os.system(cmd)
print('SEND %.1f seconds' % (time.time() - t))

sys.exit(0)
