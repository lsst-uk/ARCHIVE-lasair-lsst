import os,sys
import time
from socket import gethostname
from datetime import datetime
import settings
import date_nid
import run_active_queries
from check_alerts_watchlists import get_watchlist_hits

if len(sys.argv) > 1:
    nid = int(sys.argv[1])
else:
    nid  = date_nid.nid_now()

date = date_nid.nid_to_date(nid)
topic  = 'ztf_' + date + '_programid1'

######  test topic
topic = 'ztf_test_sherlock'

##### clear out the local database
os.system('date')
print('clear local caches')
cmd = 'python3 refresh.py'
os.system(cmd)

##### fetch a batch of annotated alerts
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

##### run the watchlists
hits = get_watchlist_hits()
if hits:
   for hit in hits:
        print(hit)

##### run the user queries
t = time.time()
run_active_queries.run_queries()
print('QUERIES %.1f seconds' % (time.time() - t))

##### build CSV file with local database
t = time.time()
print('SEND to ARCHIVE')
cmd = 'mysql --user=ztf --database=ztf --password=%s < output_csv.sql' % settings.DB_PASS_LOCAL
os.system(cmd)

for table in ['objects', 'sherlock_crossmatches']:
    cmd = 'mv /var/lib/mysql-files/%s.txt /home/ubuntu/scratch/%s.txt' % (table, table)
    os.system(cmd)

##### send CSV file to central database
for table in ['objects', 'sherlock_crossmatches']:
    outfile = '/home/ubuntu/scratch/%s.txt' % table
    if os.path.exists(outfile) and os.stat(outfile).st_size == 0:
        print('SEND %s file is empty' % table)
        print('SEND %.1f seconds' % (time.time() - t))
    else:
        vm = gethostname()
        cmd = 'scp /home/ubuntu/scratch/%s.txt %s:scratch/%s__%s' % (table, settings.DB_HOST_REMOTE, vm, table)
        os.system(cmd)

##### ingest CSV file to central database
        cmd = 'ssh %s "python3 /home/ubuntu/lasair-lsst/lasair-db/archive_in.py %s__%s"' % (settings.DB_HOST_REMOTE, vm, table)
        os.system(cmd)
print('SEND %.1f seconds' % (time.time() - t))

sys.exit(0)
