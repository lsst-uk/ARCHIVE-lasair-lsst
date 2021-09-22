global mm_event_jd, time_width
""" 
Toy data mining code for Lasair archive
---------------------------------------
We imagine a multimessenger event at a given time (mm_event_jd)
within a given sky rectangle (within rect_width degrees of RA=290, Dec=0)
and search for ZTF objects that have a candidate within a time window 
given by mm_event_jd - time_width to mm_event_jd + time_width.
"""
mm_event_jd   = 2459274.5  # 2021-03-01 12:00:00
time_width    = 1.0        # a day each side
rect_width    = 10.0       # width of sky rectangle
use_cephfs    = True

global cassandra_session
cassandra_session = None

import sys
import time
import mysql.connector
import json
import settings
import dask.bag as db

def find_mm_candidates(objectId, candlist):
    """ 
    Given a list of candidates, find those within the time-window
    Minimum time before and minimum time after
    """
#    print(json.dumps(candlist, indent=2))
    before = 100
    after  = 100
    for c in candlist:
        jd = c['jd']
        if jd < mm_event_jd and jd > mm_event_jd-time_width and jd > mm_event_jd-before:
            before = mm_event_jd-jd
        if jd > mm_event_jd and jd < mm_event_jd+time_width and jd < mm_event_jd+after:
            after = jd-mm_event_jd
    if before < 100 and after < 100:
        return '%s: before %.3f after %.3f\n' % (objectId, before, after)
    else:
        return ''

def mapit(objectId):
    global cassandra_session
    query = "SELECT candid, jd, ra, dec "
    query += "from candidates where objectId = '%s'" % objectId
    ret = cassandra_session.execute(query)
    candidates = []
    for cand in ret:
        candidates.append(cand)
    s = find_mm_candidates(objectId, candidates)
    return s

#######################################
def main():
    global mm_event_jd, time_width, search_out
    global cassandra_session

    if len(sys.argv) < 3:
        print('usage: mmsearch rect_width time_width')
        sys.exit()
    
    rect_width = float(sys.argv[1])
    time_width = float(sys.argv[2])
    #print('rect_width %.0f time_width %.0f ' % (rect_width, time_width))
    
    msl = mysql.connector.connect(
        user    = settings.READONLY_USER,
        password= settings.READONLY_PASS,
        host    = settings.DB_HOST,
        database='ztf')
    cursor = msl.cursor(buffered=True, dictionary=True)
    
    query = "SELECT objects.objectId FROM objects "
    query += "WHERE jdmin < %f and jdmax > %f " % (mm_event_jd, mm_event_jd)
    query += "AND ramean between %.1f and %.1f AND decmean between %.1f and %.1f" % \
        (290-rect_width, 290+rect_width, -rect_width, rect_width)
    
    #print(query)
    cursor.execute(query)


    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    host = ['192.168.0.17', '192.168.0.11', '192.168.0.18', '192.168.0.39', '192.168.0.23']
    table = 'candidates'
    cluster = Cluster(host)
    cassandra_session = cluster.connect()
    cassandra_session.row_factory = dict_factory
    cassandra_session.set_keyspace('lasair')

    objectIdList = []
    for row in cursor:
       objectIdList.append(row['objectId'])
    nobject = len(objectIdList)
    print('%d objects' % nobject)

    from dask.distributed import Client
    client = Client()
    t = time.time()
    bag = db.from_sequence(objectIdList, npartitions=4)
    mapped_bag = bag.filter(mapit)
    filtered_bag = mapped_bag.filter(lambda s: len(s) > 0)
    objectIds = filtered_bag.compute(scheduler='threads')
    t = time.time() - t

    out_file = open('out/out_%.0f_%.0f.txt' % (rect_width, time_width), 'w')
    for objectId in objectIds:
        out_file.write(objectId + '\n')
    out_file.close()
    print('%d files in %.1f msec each' % (nobject, t*1000.0/nobject))

    if not use_cephfs:
        cluster.shutdown()

if __name__ == "__main__":
    main()
