""" This is the master script that does these steps:
    -- fetch a batch of alerts from kafka
    -- run the watchlist code and insert the hits
    -- run the active user queries and produce kafka
    -- build a CSV file of three tables with the batch: 
        objects, sherlock_crossmatches, watchlist_hits
    -- scp those files to lasair-db
    -- ssh to ingest those files to master database
"""
import os,sys
import time
from socket import gethostname
from datetime import datetime
import settings
import date_nid
import run_active_queries
from check_alerts_watchlists import get_watchlist_hits, insert_watchlist_hits

# if argument is test, use test stream
if len(sys.argv) > 1 and sys.argv[1] == 'test':
    topic = 'ztf_test_sherlock'    #  test topic 

else:
# if argument is number assume it is a nid (night id number)
# if no argument, assume today
    if len(sys.argv) > 1: nid = int(sys.argv[1])
    else:                 nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'

##### clear out the local database
os.system('date')
print('clear local caches')
cmd = 'python3 refresh.py'
os.system(cmd)

##### fetch a batch of annotated alerts
print('INGEST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
print("Topic is %s" % topic)
t = time.time()

cmd =  'python3 ingestStreamThreaded.py '
cmd += '--maxalert %d ' % settings.KAFKA_MAXALERTS
cmd += '--nthread %d '  % settings.KAFKA_THREADS
cmd += '--group %s '    % settings.KAFKA_GROUPID
cmd += '--host %s '     % settings.KAFKA_PRODUCER
cmd += '--topic ' + topic

print(cmd)
# rc is the return code from ingestion, 1 if no more to get, 0 if got full batch
rc = os.system(cmd)
print('INGEST duration %.1f seconds' % (time.time() - t))

##### run the watchlists
print('WATCHLIST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
t = time.time()
hits = get_watchlist_hits()
print('got %d watchlist hits' % len(hits))
if len(hits) > 0:
    insert_watchlist_hits(hits)
print('WATCHLIST %.1f seconds' % (time.time() - t))

##### run the user queries
print('QUERIES start %s' % datetime.utcnow().strftime("%H:%M:%S"))
t = time.time()
run_active_queries.run_queries()
print('QUERIES %.1f seconds' % (time.time() - t))

##### build CSV file with local database
t = time.time()
print('SEND to ARCHIVE')
cmd = 'mysql --user=ztf --database=ztf --password=%s < output_csv.sql' % settings.DB_PASS_LOCAL
os.system(cmd)

tablelist = ['objects', 'sherlock_crossmatches', 'watchlist_hits']
for table in tablelist:
    cmd = 'mv /var/lib/mysql-files/%s.txt /home/ubuntu/scratch/%s.txt' % (table, table)
    os.system(cmd)

##### send CSV file to central database
for table in tablelist:
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

sys.exit(rc)
