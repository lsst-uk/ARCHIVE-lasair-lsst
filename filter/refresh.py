""" Clear out the local database
"""
import sys
import settings
import mysql.connector
config = {
    'user'    : settings.DB_USER_LOCAL,
    'password': settings.DB_PASS_LOCAL,
    'host'    : settings.DB_HOST_LOCAL,
    'database': 'ztf'
}
try:
    msl = mysql.connector.connect(**config)
    cursor = msl.cursor(buffered=True, dictionary=True)
except:
    print('ERROR in filter/refresh: cannot clear local database')
    sys.stdout.flush()

query = 'TRUNCATE TABLE objects'
cursor.execute(query)

query = 'TRUNCATE TABLE sherlock_classifications'
cursor.execute(query)

query = 'TRUNCATE TABLE watchlist_hits'
cursor.execute(query)

query = 'TRUNCATE TABLE area_hits'
cursor.execute(query)

