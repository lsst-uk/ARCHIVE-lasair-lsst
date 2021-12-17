""" Check that the stored queries in the datbase can all be modified to have
a specific object constraint, and the query is still syntactially correct.
"""
import sys
sys.path.append('../filter')
from run_active_queries import fetch_queries, query_for_object, db_connect_remote

objectId = 'ZTF12aaaaaa'
query_list = fetch_queries()

msl_remote = db_connect_remote()
cursor = msl_remote.cursor(buffered=True, dictionary=True)

for q in query_list:
    query = q['real_sql']
    print('====== mq_id ', q['mq_id'])
    query_obj = query_for_object(query, objectId)

    try:
        cursor.execute(query + ' LIMIT 0')
    except Exception as e:
        print('------- Plain query failed\n' + str(e))
        print(query)

    try:
        cursor.execute(query_obj + ' LIMIT 0')
    except Exception as e:
        print('------- With object query failed\n' + str(e))
        print(query_obj)
