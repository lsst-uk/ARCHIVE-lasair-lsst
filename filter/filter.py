""" This is the master script that does these steps:
    -- fetch a batch of alerts from kafka
    -- run the watchlist code and insert the hits
    -- run the active user queries and produce kafka
    -- build a CSV file of three tables with the batch: 
        objects, sherlock_classifications, watchlist_hits, area_hits
    -- scp those files to lasair-db
    -- ssh to ingest those files to master database
"""
import os,sys
import time
from socket import gethostname
from datetime import datetime
import settings
sys.path.append('../utility/')
from manage_status import manage_status
import run_active_queries
from check_alerts_watchlists import get_watchlist_hits, insert_watchlist_hits
from check_alerts_areas import get_area_hits, insert_area_hits
from counts import since_midnight, grafana_today
import mysql.connector

def db_connect():
    """db_connect.
    """
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    msl_local = mysql.connector.connect(**config)
    return msl_local

topic = settings.KAFKA_TOPIC_IN

print('------------------')
##### clear out the local database
os.system('date')
print('clear local caches')
sys.stdout.flush()
cmd = 'python3 refresh.py'
os.system(cmd)

##### fetch a batch of annotated alerts
print('INGEST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
print("Topic is %s" % topic)
t = time.time()

cmd =  'python3 ingestStreamThreaded.py '
cmd += '--maxalert %d ' % settings.KAFKA_MAXALERTS
cmd += '--nprocess %d ' % settings.KAFKA_PROCESSES
cmd += '--group %s '    % settings.KAFKA_GROUPID
cmd += '--host %s '     % settings.KAFKA_HOST
cmd += '--topic ' + topic

print(cmd)
# rc is the return code from ingestion, number of alerts received
rc = os.system(cmd)
print('INGEST duration %.1f seconds' % (time.time() - t))

try:
    msl = db_connect()
except:
    print('ERROR in filter/filter: cannot connect to local database')
    sys.stdout.flush()

##### run the watchlists
print('WATCHLIST start %s' % datetime.utcnow().strftime("%H:%M:%S"))
sys.stdout.flush()
t = time.time()
hits = get_watchlist_hits(msl, settings.WATCHLIST_MOCS, settings.WATCHLIST_CHUNK)
print('got %d watchlist hits' % len(hits))
sys.stdout.flush()
if len(hits) > 0:
    insert_watchlist_hits(msl, hits)
print('WATCHLIST %.1f seconds' % (time.time() - t))
sys.stdout.flush()

##### run the areas
print('AREA start %s' % datetime.utcnow().strftime("%H:%M:%S"))
sys.stdout.flush()
t = time.time()
hits = get_area_hits(msl, settings.AREA_MOCS)
print('got %d area hits' % len(hits))
sys.stdout.flush()
if len(hits) > 0:
    insert_area_hits(msl, hits)
print('AREA %.1f seconds' % (time.time() - t))
sys.stdout.flush()

##### run the user queries
print('QUERIES start %s' % datetime.utcnow().strftime("%H:%M:%S"))
sys.stdout.flush()
t = time.time()
run_active_queries.run_queries()
print('QUERIES %.1f seconds' % (time.time() - t))
sys.stdout.flush()

##### build CSV file with local database
t = time.time()
print('SEND to ARCHIVE')
sys.stdout.flush()
cmd = 'rm /home/ubuntu/csvfiles/*'
os.system(cmd)
cmd = 'mysql --user=ztf --database=ztf --password=%s < output_csv.sql' % settings.DB_PASS_LOCAL
if os.system(cmd) > 0:
    print('ERROR in filter/filter: cannot build CSV from local database')
    sys.stdout.flush()

tablelist = ['objects', 'sherlock_classifications', 'watchlist_hits', 'area_hits']

##### send CSV file to central database
t = time.time()
for table in tablelist:
    outfile = '/home/ubuntu/csvfiles/%s.txt' % table
    if os.path.exists(outfile) and os.stat(outfile).st_size == 0:
        print('SEND %s file is empty' % table)
        sys.stdout.flush()
    else:
        vm = gethostname()
        cmd = 'scp /home/ubuntu/csvfiles/%s.txt %s:scratch/%s__%s' % (table, settings.DB_HOST_REMOTE, vm, table)
        os.system(cmd)
        if os.system(cmd) > 0:
            print('ERROR in filter/filter: cannot copy CSV to master database node')
            sys.stdout.flush()

##### ingest CSV file to central database
        cmd = 'ssh %s "python3 /home/ubuntu/lasair-lsst/lasair-db/archive_in.py %s__%s"' % (settings.DB_HOST_REMOTE, vm, table)
        if os.system(cmd) > 0:
            print('ERROR in filter/filter: cannot ingest CSV on master database node')
            sys.stdout.flush()
print('Transfer to master %.1f seconds' % (time.time() - t))
sys.stdout.flush()

ms = manage_status('nid', settings.SYSTEM_STATUS)
d = since_midnight()
ms.set({
    'today_ztf':grafana_today(), 
    'today_database':d['count'], 
    'min_delay':d['delay'], 
    'total_count': d['total_count']})
print('Exit status', rc)
sys.stdout.flush()
if rc > 0: sys.exit(1)
else:      sys.exit(0)
