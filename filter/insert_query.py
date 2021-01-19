""" Computes features of the light curve and builds and object record
"""
from __future__ import print_function
from mag import dc_mag_dict
import json
import math
import numpy as np
import ephem
from gkhtm import _gkhtm as htmCircle
import settings
import cassandra_import

def make_ema(candlist):
    """make_ema.
    Make a exponential moving average (EMA)
        https://lasair.roe.ac.uk/lasair/static/EMA.pdf
    from the apparent magnitudes.
    candlist is the list of candidates in time order.

    Args:
        candlist
    """
    oldgjd = oldrjd = 0
    g02 = g08 = g28 = 0
    r02 = r08 = r28 = 0
    mag_g = mag_r = 0
    n = 0
    for c in candlist:
        jd = c['jd']
        if not c['magpsf']:
            continue
#        d = dc_mag_dict(
#            c['fid'], 
#            c['magpsf'], c['sigmapsf'], 
#            c['magnr'],  c['sigmagnr'], 
#            c['magzpsci'], c['isdiffpos'])
        # compute the apparent (DC) magnitude
#        dc_mag = d['dc_mag']
        mag = c['magpsf']

        # separate the g mag (fid=1) from r mag (fid=2)
        if c['fid'] == 1:
            f02 = math.exp(-(jd-oldgjd)/2.0)
            f08 = math.exp(-(jd-oldgjd)/8.0)
            f28 = math.exp(-(jd-oldgjd)/28.0)
            g02 = g02*f02 + mag*(1-f02)
            g08 = g08*f08 + mag*(1-f08)
            g28 = g28*f28 + mag*(1-f28)
            oldgjd = jd
        else:
            f02 = math.exp(-(jd-oldrjd)/2.0)
            f08 = math.exp(-(jd-oldrjd)/8.0)
            f28 = math.exp(-(jd-oldrjd)/28.0)
            r02 = r02*f02 + mag*(1-f02)
            r08 = r08*f08 + mag*(1-f08)
            r28 = r28*f28 + mag*(1-f28)
            oldrjd = jd
    ema = { 
        'g02':g02, 'g08':g08, 'g28':g28, 
        'r02':r02, 'r08':r08, 'r28':r28
        }
    return ema 

def insert_cassandra(alert):
    """insert_casssandra.
    Creates an insert for cassandra
    a query for inserting it.

    Args:
        alert:
    """

    # if this is not set, then we are not doing cassandra
    try:
        if len(settings.CASSANDRA_HEAD) == 0: return 0
    except:
        return 0

    from cassandra.cluster import Cluster

    objectId =  alert['objectId']

    candlist = None
    # Make a list of candidates and noncandidates in time order
    if 'candidate' in alert and alert['candidate'] != None:
        if 'prv_candidates' in alert and alert['prv_candidates'] != None:
            candlist = alert['prv_candidates'] + [alert['candidate']]
        else:
            candlist = [alert['candidate']]

    # will be list of real detections, each has a non-null candid
    detectionCandlist = []

    for cand in candlist:
        if cand['candid'] is None: 
            continue
        cand['objectId'] = objectId
        detectionCandlist.append(cand)

    if len(detectionCandlist) == 0:
        return 0

    # connect to cassandra cluster
    try:
        cluster = Cluster(settings.CASSANDRA_HEAD)
        session = cluster.connect()
        session.set_keyspace('lasair')
    except Exception as e:
        print("Cassandra connection failed for %s" % str(settings.CASSANDRA_HEAD))
        print(e)
        return 0

    # Add the htm16 IDs in bulk. Could have done it above as we iterate through the candidates,
    # but the new C++ bulk code is 100 times faster than doing it one at a time.
    htm16s = htmCircle.htmIDBulk(16, [[x['ra'],x['dec']] for x in detectionCandlist])

    # Now add the htmid16 value into each dict.
    for i in range(len(detectionCandlist)):
        detectionCandlist[i]['htmid16'] = htm16s[i]

    cassandra_import.loadGenericCassandraTable(session, 'candidates', detectionCandlist)
    cluster.shutdown()
    return len(detectionCandlist)

def mymax(a, b):
    if not a: return b
    if not b: return a
    if a > b: return a
    else:     return b

def create_insert_query(alert):
    """create_insert_query.
    Creates an insert sql statement for building the object and 
    a query for inserting it.

    Args:
        alert:
    """
    objectId =  alert['objectId']

    candlist = None
    # Make a list of candidates and noncandidates in time order
    if 'candidate' in alert and alert['candidate'] != None:
        if 'prv_candidates' in alert and alert['prv_candidates'] != None:
            candlist = alert['prv_candidates'] + [alert['candidate']]
        else:
            candlist = [alert['candidate']]

#    s = '-'
#    for c in candlist:
#        if c['candid'] is None: s += 'N'
#        else: s += 'D'
#    print(s)

    if not candlist: return None
    ema = make_ema(candlist)

    ncand = 0
    jdmin = 3000000000.0
    ra = []
    dec = []
    magg = []
    magr = []
    jdg   = []
    jdr   = []
    latestgmag = latestrmag = 'NULL'
    ncandgp = 0
    sgmag1    = None
    srmag1    = None
    sgscore1  = None
    distpsnr1 = None

    for cand in candlist:
        # if this is a real detection, it will have a candid else nondetection
        if cand['candid'] is None: continue

        ra.append(cand['ra'])
        dec.append(cand['dec'])
        if cand['jd'] < jdmin:
            jdmin = cand['jd']
        if cand['fid'] == 1:
            magg.append(cand['magpsf'])
            jdg.append(cand['jd'])
            latestgmag = cand['magpsf']
        else:
            magr.append(cand['magpsf'])
            jdr.append(cand['jd'])
            latestrmag = cand['magpsf']

        # if it also has the 'drb' data quality flag, copy the PS1 data
        if 'drb' in cand:
            sgmag1    = cand['sgmag1']
            srmag1    = cand['srmag1']
            sgscore1  = cand['sgscore1']
            distpsnr1 = cand['distpsnr1']
        if cand['rb'] > 0.75 and cand['isdiffpos'] == 't':
            ncandgp += 1
        ncand += 1

    # only want light curves with at least 2 candidates
    if ncand <= 1:
        return None

    # statistics of the g light curve
    dmdt_g = dmdt_g_2 = 'NULL'
    if len(magg) > 0:
        maggmin = np.min(magg)
        maggmax = np.max(magg)
        maggmean = np.mean(magg)
        try:     dmdt_g   = (magg[-2] - magg[-1])/(jdg[-1] - jdg[-2])
        except:  pass
        try:     dmdt_g_2 = (magg[-3] - magg[-2])/(jdg[-2] - jdg[-3])
        except:  pass
    else:
        maggmin = maggmax = maggmean = maggmedian = 'NULL'

    # statistics of the r light curve
    dmdt_r = dmdt_r_2 = 'NULL'
    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        try:     dmdt_r   = (magr[-2] - magr[-1])/(jdr[-1] - jdr[-2])
        except:  pass
        try:     dmdt_r_2 = (magr[-3] - magr[-2])/(jdr[-2] - jdr[-3])
        except:  pass
    else:
        magrmin = magrmax = magrmean = magrmedian = 'NULL'

    # mean position
    ramean  = np.mean(ra)
    decmean = np.mean(dec)

    # galactic coordinates
    ce = ephem.Equatorial(math.radians(ramean), math.radians(decmean))
    cg = ephem.Galactic(ce)
    glonmean = math.degrees(float(repr(cg.lon)))
    glatmean = math.degrees(float(repr(cg.lat)))

    # Compute the HTM ID for later cone searches
    try:
        htm16 = htmCircle.htmID(16, ramean, decmean)
    except:
        htm16 = 0
        print('Cannot get HTMID for ra=%f, dec=%f' % (ramean, decmean))

    # dictionary of attributes
    sets = {}
    sets['ncand']      = ncand
    sets['ramean']     = ramean
    sets['rastd']      = 3600*np.std(ra)
    sets['decmean']    = decmean
    sets['decstd']     = 3600*np.std(dec)
    sets['maggmin']    = maggmin
    sets['maggmax']    = maggmax
    sets['maggmean']   = maggmean
    sets['magrmin']    = magrmin
    sets['magrmax']    = magrmax
    sets['magrmean']   = magrmean
    sets['gmag']       = latestgmag
    sets['rmag']       = latestrmag
    sets['dmdt_g']     = dmdt_g
    sets['dmdt_r']     = dmdt_r
    sets['dmdt_g_2']   = dmdt_g_2
    sets['dmdt_r_2']   = dmdt_r_2
    sets['jdmin']      = jdmin
    if len(jdg) > 0: sets['jdgmax'] = np.max(jdg)
    else:            sets['jdgmax'] = 'NULL'
    if len(jdr) > 0: sets['jdrmax'] = np.max(jdr)
    else:            sets['jdrmax'] = 'NULL'
    sets['jdmax']      = mymax(sets['jdgmax'], sets['jdgmax'])
    sets['glatmean']   = glatmean
    sets['glonmean']   = glonmean

    # pannstarrs
    sets['sgmag1']     = sgmag1
    sets['srmag1']     = srmag1
    sets['sgscore1']   = sgscore1
    sets['distpsnr1']  = distpsnr1
    sets['ncandgp']    = ncandgp

    # HTM id
    sets['htm16']      = htm16

    # Moving averages
    sets['mag_g02'] = ema['g02']
    sets['mag_g08'] = ema['g08']
    sets['mag_g28'] = ema['g28']
    sets['mag_r02'] = ema['r02']
    sets['mag_r08'] = ema['r08']
    sets['mag_r28'] = ema['r28']

    # Make the query
    list = []
    query = 'REPLACE INTO objects SET objectId="%s", ' % objectId
    for key,value in sets.items():
        if value == 'NULL':
            list.append(key + '= NULL')
        elif isinstance(value, str):
            list.append(key + '= "' + str(value) + '"')
        else:
            list.append(key + '=' + str(value))
    query += ', '.join(list)
    return query

def create_insert_annotation(msl, objectId, annClass, ann, attrs, table, replace):
    """create_insert_annotation.
    This code makes the insert query for the genaric annotation

    Args:
        msl:
        objectId:
        annClass:
        ann:
        attrs:
        table:
        replace:
    """
    sets = {}
    for key in attrs:
        sets[key] = 0
    for key, value in ann.items():
        if key in attrs and value:
            sets[key] = value
    if 'description' in attrs and not 'description' in ann:
        sets['description'] = 'no description'
    # Build the query
    list = []
    if replace: query = 'REPLACE'
    else:       query = 'INSERT'
    query += ' INTO %s SET ' % (table)
    for key,value in sets.items():
#        if isinstance(value, str):
        list.append(key + '=' + "'" + str(value).replace("'", '') + "'")
#    else:
#        list.append(key + '=' + str(value))
    query += ', '.join(list)
    query = query.replace('None', 'NULL')
#    print('=====')
#    print(ann)
#    print('--')
#    print(query)
#    print('=====')
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
        msl.commit()
    except mysql.connector.Error as err:
        print('INGEST %s Database insert candidate failed: %s' % (annClass, str(err)))

import os
if __name__ == '__main__':
    root = '/mnt/cephfs/roy/lightcurve/'
    for hex3 in os.listdir(root):
        for jsonfile in os.listdir(root + hex3):
            filename = root + hex3 + '/' + jsonfile
            alert = json.loads(open(filename).read()) 
            query = create_insert_query(alert)
            if query:
                print(query + ';\n')
