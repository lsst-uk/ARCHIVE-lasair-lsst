import sys
import settings
import json
import mysql.connector
from subprocess import Popen, PIPE

config = {
    'user'    : settings.DB_USER_REMOTE,
    'password': settings.DB_PASS_REMOTE,
    'host'    : settings.DB_HOST_REMOTE,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

cursor = msl.cursor(buffered=True, dictionary=True)
query = 'SELECT tns_name FROM crossmatch_tns WHERE tns_name NOT IN '
query += '(SELECT name FROM watchlist_cones WHERE wl_id=%d)' % settings.TNS_WATCHLIST_ID

cursor.execute(query)
n = 0
for row in cursor:
    print(row['tns_name'])
    n += 1

assert(n==0)
print('Every crossmatch_tns is in watchlist_cones')
