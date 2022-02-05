import os,sys
import time
import settings
import mysql.connector
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from insert_query import create_insert_query_from_list

def make_database_connection():
    """make_database_connection.
    """
    # Make a connection to the *local* database to put these
    msl = mysql.connector.connect(
        user     = settings.DB_USER_LOCAL,
        password = settings.DB_PASS_LOCAL,
        host     = settings.DB_HOST_LOCAL,
        database = settings.DB_DATABASE,
        )
    return msl

def execute_query(query, msl):
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
        msl.commit()
    except mysql.connector.Error as err:
        print('ERROR filter/consume_alerts: object Database insert candidate failed: %s' % str(err))
        print(query)
        sys.stdout.flush()

def rebuild_features(objectIds, cassandra_session, msl):
    for objectId in objectIds:
        cass_query = "SELECT * from candidates where objectId = '%s'" % objectId
        ret = cassandra_session.execute(cass_query)
        candidates = []
        for cand in ret:
            candidates.append(cand)

        querydict = create_insert_query_from_list(objectId, candidates)
        if querydict['ss'] == 0:
            execute_query(querydict['query'], msl)

if __name__ == "__main__":
    objectIds = [
    'ZTF22aaaesqq', 'ZTF22aaaesjw', 'ZTF22aaaerzx', 'ZTF21acowkut', 'ZTF21aciqzgw',
    'ZTF21abcyxmm', 'ZTF21aalokic', 'ZTF20actwpjk', 'ZTF20acqocit', 'ZTF20acnvzwx',
    'ZTF20abisxyz', 'ZTF20aaxspuz', 'ZTF20aaodqgg', 'ZTF20aanadrk', 'ZTF20aameagt',
    ]

    host = ['192.168.0.17', '192.168.0.11', '192.168.0.18', '192.168.0.39', '192.168.0.23']
    table = 'candidates'
    cluster = Cluster(host)
    cassandra_session = cluster.connect()
    cassandra_session.row_factory = dict_factory
    cassandra_session.set_keyspace('lasair')

    msl = make_database_connection()

    t = time.time()
    rebuild_features(objectIds, cassandra_session, msl)
    print("Rebuilt %d objects in %.2f seconds" % (len(objectIds), (time.time()-t)))
