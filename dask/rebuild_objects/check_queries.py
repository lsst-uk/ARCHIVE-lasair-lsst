import sys
import settings
import mysql.connector
sys.path.append('../../../lasair-lsst-web/src/lasair-webapp/lasair/lasair')
sys.path.append('../../../lasair-lsst-web/src/lasair-webapp/lasair')
from query_builder import check_query, build_query

msl = mysql.connector.connect(
    user    = settings.READONLY_USER,
    password= settings.READONLY_PASS,
    host    = settings.LASAIR_DB,
    port    = settings.LASAIR_PORT,
    database='ztf')
cursor  = msl.cursor(buffered=True, dictionary=True)
cursor2 = msl.cursor(buffered=True, dictionary=True)

query = 'SELECT mq_id, selected, conditions, tables FROM myqueries'
cursor.execute(query)

for row in cursor:
    mq_id = row['mq_id']
    s = row['selected']
    f = row['tables']
    w = row['conditions']

    message = check_query(s, f, w)
    if message:
        print(mq_id, message)
        continue
    else:
        real_sql = build_query(s, f, w)

    try:
        cursor2.execute(real_sql + ' LIMIT 0')
        message = None
    except Exception as e:
        message = str(e)

    if message:
        print(mq_id, message)
    else:
        print(mq_id, 'OK')
