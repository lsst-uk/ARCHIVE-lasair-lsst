""" Computes features of the light curve and builds and object record
"""
from __future__ import print_function
from mag import dc_mag_dict
import json
import sys
import math
import numpy as np
import ephem
from gkhtm import _gkhtm as htmCircle

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
        if not 'magpsf' in c:
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

def mymax(a, b):
    if a and b:
        if a > b: return a
        else:     return b
    if a:
        return a
    else:
        return b

def create_insert_query(alert):
    """create_insert_query.
    Creates an insert sql statement for building the object and 
    a query for inserting it.

    Args:
        alert:
    """
    objectId =  alert['objectId']

    # Make a list of candidates and noncandidates in time order
    if 'candidate' in alert and alert['candidate'] != None:
        if 'prv_candidates' in alert and alert['prv_candidates'] != None:
            clist = alert['prv_candidates'] + [alert['candidate']]
        else:
            clist = [alert['candidate']]

    candlist = []
    for cand in clist:
        if 'candid' in cand and cand['candid']:
            candlist.append(cand)

    if not candlist: return None

    sets = create_features(objectId, candlist)
    if not sets:
        return None

    # Make the query
    list = []
    query = 'REPLACE INTO objects SET objectId="%s", ' % objectId
    for key,value in sets.items():
        if not value:
            list.append(key + '= NULL')
        elif isinstance(value, str):
            list.append(key + '= "' + str(value) + '"')
        else:
            list.append(key + '=' + str(value))
    query += ',\n'.join(list)

    if sets['ssnamenr']: ss = 1
    else:                ss = 0
    return {'ss':ss, 'query':query}

def good(cand):
    if 'rb' in cand and cand['rb'] and cand['rb'] > 0.75:
        return True
    if 'drb' in cand and cand['drb'] and cand['drb'] > 0.75:
        return True
    return False

def create_features(objectId, candlist):
    # version 1.0
    ema = make_ema(candlist)

    ncand = 0
    jdmin = 3000000000.0
    ra = []
    dec = []
    magg = []
    magr = []
    jdg   = []
    jdr   = []
    latestgmag = latestrmag = None
    sgmag1    = None
    srmag1    = None
    sgscore1  = None
    distpsnr1 = None
    ssnamenr = None

    g_nid = {}
    r_nid = {}
    for cand in candlist:
        # if this is a real detection, it will have a candid else nondetection
        if not 'candid' in cand or not cand['candid']: 
            continue

        nid = cand['nid']
        ra.append(cand['ra'])
        dec.append(cand['dec'])
        if cand['jd'] < jdmin:
            jdmin = cand['jd']
        if cand['fid'] == 1:
            magg.append(cand['magpsf'])
            jdg.append(cand['jd'])
            latestgmag = cand['magpsf']
            if cand['isdiffpos'] == 't':
                g_nid[nid] = (cand['magpsf'], cand['jd'])
        else:
            magr.append(cand['magpsf'])
            jdr.append(cand['jd'])
            latestrmag = cand['magpsf']
            if cand['isdiffpos'] == 't':
                r_nid[nid] = (cand['magpsf'], cand['jd'])

        # if it also has the 'drb' data quality flag, copy the PS1 data
        if 'sgmag1' in cand:
            sgmag1    = cand['sgmag1']
            srmag1    = cand['srmag1']
            sgscore1  = cand['sgscore1']
            distpsnr1 = cand['distpsnr1']
        if 'drb' in cand:
            drb = cand['drb']

        ssnamenr = cand['ssnamenr']
        if ssnamenr == 'null':
            ssnamenr = None

        ncand += 1

    # if non-solar-system and one-night stand then reject
    if not ssnamenr and ncand <= 1:
        return None

    if len(jdg) > 0: jdgmax = max(jdg)
    else:            jdgmax = None
    if len(jdr) > 0: jdrmax = max(jdr)
    else:            jdrmax = None
    jdmax            = mymax(jdgmax, jdrmax)

    ncandgp = ncandgp_7 = ncandgp_14 = 0
    for cand in candlist:
        if not 'candid'in cand: 
            continue

        if good(cand) and cand['isdiffpos'] == 't' and jdmax and cand['jd']:
            ncandgp += 1
            age = jdmax - cand['jd']
            if age < 7.0:  ncandgp_7 += 1
            if age < 14.0: ncandgp_14 += 1

    g_minus_r = None
    jd_g_minus_r = None
    for nid in r_nid.keys():
        if nid in g_nid.keys():
            g_minus_r = g_nid[nid][0] - r_nid[nid][0]
            jd_g_minus_r = g_nid[nid][1]

    # statistics of the g light curve
    dmdt_g = dmdt_g_2 = None
    if len(magg) > 0:
        maggmin = np.min(magg)
        maggmax = np.max(magg)
        maggmean = np.mean(magg)
        try:     dmdt_g   = (magg[-2] - magg[-1])/(jdg[-1] - jdg[-2])
        except:  pass
        try:     dmdt_g_2 = (magg[-3] - magg[-2])/(jdg[-2] - jdg[-3])
        except:  pass
    else:
        maggmin = maggmax = maggmean = maggmedian = None

    # statistics of the r light curve
    dmdt_r = dmdt_r_2 = None
    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        try:     dmdt_r   = (magr[-2] - magr[-1])/(jdr[-1] - jdr[-2])
        except:  pass
        try:     dmdt_r_2 = (magr[-3] - magr[-2])/(jdr[-2] - jdr[-3])
        except:  pass
    else:
        magrmin = magrmax = magrmean = magrmedian = None

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
        print('ERROR: filter/insert_query: Cannot compute HTM index')
        sys.stdout.flush()

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
    sets['jdgmax']     = jdgmax
    sets['jdrmax']     = jdrmax
    sets['jdmax']      = jdmax
    sets['jdmin']      = jdmin

    sets['g_minus_r']      = g_minus_r
    sets['jd_g_minus_r']   = jd_g_minus_r

    sets['glatmean']   = glatmean
    sets['glonmean']   = glonmean

    # miscellaneous
    sets['sgmag1']     = sgmag1
    sets['srmag1']     = srmag1
    sets['sgscore1']   = sgscore1
    sets['distpsnr1']  = distpsnr1
    sets['ssnamenr']   = ssnamenr
    sets['ncandgp']    = ncandgp
    sets['ncandgp_7']  = ncandgp_7
    sets['ncandgp_14'] = ncandgp_14

    # HTM id
    sets['htm16']      = htm16

    # Moving averages
    sets['mag_g02'] = ema['g02']
    sets['mag_g08'] = ema['g08']
    sets['mag_g28'] = ema['g28']
    sets['mag_r02'] = ema['r02']
    sets['mag_r08'] = ema['r08']
    sets['mag_r28'] = ema['r28']
    return sets

def create_insert_annotation(objectId, annClass, ann, attrs, table, replace):
    """create_insert_annotation.
    This code makes the insert query for the genaric annotation

    Args:
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
    query += ',\n'.join(list)
    query = query.replace('None', 'NULL')
    return query
