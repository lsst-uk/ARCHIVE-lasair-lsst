""" Clear out the local database
"""
import settings
import mysql.connector
config = {
    'user'    : settings.DB_USER_LOCAL,
    'password': settings.DB_PASS_LOCAL,
    'host'    : settings.DB_HOST_LOCAL,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)
cursor = msl.cursor(buffered=True, dictionary=True)

query = 'TRUNCATE TABLE objects'
cursor.execute(query)

query = 'TRUNCATE TABLE sherlock_crossmatches'
cursor.execute(query)

query = 'TRUNCATE TABLE watchlist_hits'
cursor.execute(query)

