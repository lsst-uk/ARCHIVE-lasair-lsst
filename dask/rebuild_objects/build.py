import os,sys
import time
import settings
import mysql.connector
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from check_schema import get_schema_names

sys.path.append('../../filter')
from insert_query import create_features

def get_cassandra_session():
    host = ['192.168.0.17', '192.168.0.11', '192.168.0.18', '192.168.0.39', '192.168.0.23']
    table = 'candidates'
    cluster = Cluster(host)
    cassandra_session = cluster.connect()
    cassandra_session.row_factory = dict_factory
    cassandra_session.set_keyspace('lasair')
    return cassandra_session

def rebuild_features(d):
    objectId          = d['objectId']
    schema_names      = d['schema_names']
    cassandra_session = d['cassandra_session']

    cass_query = "SELECT * FROM candidates WHERE objectId = '%s'" % objectId
    ret = cassandra_session.execute(cass_query)

    # We need to restrict the lightcurve to the last 30 days of detections
    clist = []
    for c in ret:
        clist.append(c)
    clist.sort(key = lambda c: -c['jd'])
    jdmax = clist[0]['jd']
    candidates = []
    for c in clist:
        if (jdmax - c['jd']) < 30:    # ZTF alerts have at most 30 days of candidates
            candidates.append(c)

    attrs = create_features(objectId, candidates)
    if not attrs:
        return None
    attrs['objectId'] = objectId
    slist = []
    for attr in schema_names:
        try:
            a = attrs[attr]
        except:
            print('ERROR %s not in features for object %s !!' % (attr, objectId))
            return None
#        print(attr, a)
        if a: slist.append('"%s"' % str(a))
        else: slist.append('\\N')
    return ','.join(slist)

if __name__ == "__main__":
    objectIds = ["ZTF17aaadzrz", "ZTF17aaaewks", "ZTF17aaagqzj", "ZTF17aaahdxk"]
    cassandra_session = get_cassandra_session()
    schema_names = get_schema_names()

    nobj = 0
    t = time.time()
    for objectId in objectIds:
        s = rebuild_features({
            'objectId': objectId,
            'schema_names': schema_names,
            'cassandra_session':cassandra_session,
        })
        if s:
            print(s)
            nobj += 1
    print("Rebuilt %d of %d objects in %.2f seconds" % (nobj, len(objectIds), (time.time()-t)))
