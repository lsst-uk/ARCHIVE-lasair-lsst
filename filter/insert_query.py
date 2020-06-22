""" Computes features of the light curve and builds and object record
"""
from __future__ import print_function
from mag import dc_mag_dict
import json
import math
import numpy as np
import ephem
from gkhtm import _gkhtm as htmCircle

def make_ema(candlist):
    """ Make a exponential moving average (EMA)
        https://lasair.roe.ac.uk/lasair/static/EMA.pdf
    from the apparent magnitudes.
    candlist is the list of candidates in time order.
    """
    oldgjd = oldrjd = 0
    g02 = g08 = g28 = 0
    r02 = r08 = r28 = 0
    dc_mag_g = dc_mag_r = 0
    n = 0
    for c in candlist:
        jd = c['jd']
        if not c['magpsf']:
            continue
        d = dc_mag_dict(
            c['fid'], 
            c['magpsf'], c['sigmapsf'], 
            c['magnr'],  c['sigmagnr'], 
            c['magzpsci'], c['isdiffpos'])
        # compute the apparent (DC) magnitude
        dc_mag = d['dc_mag']

        # separate the g mag (fid=1) from r mag (fid=2)
        if c['fid'] == 1:
            f02 = math.exp(-(jd-oldgjd)/2.0)
            f08 = math.exp(-(jd-oldgjd)/8.0)
            f28 = math.exp(-(jd-oldgjd)/28.0)
            g02 = g02*f02 + dc_mag*(1-f02)
            g08 = g08*f08 + dc_mag*(1-f08)
            g28 = g28*f28 + dc_mag*(1-f28)
            oldgjd = jd
        else:
            f02 = math.exp(-(jd-oldrjd)/2.0)
            f08 = math.exp(-(jd-oldrjd)/8.0)
            f28 = math.exp(-(jd-oldrjd)/28.0)
            r02 = r02*f02 + dc_mag*(1-f02)
            r08 = r08*f08 + dc_mag*(1-f08)
            r28 = r28*f28 + dc_mag*(1-f28)
            oldrjd = jd
    ema = { 
        'dc_mag_g':dc_mag_g, 'g02':g02, 'g08':g08, 'g28':g28, 
        'dc_mag_r':dc_mag_r, 'r02':r02, 'r08':r08, 'r28':g28
        }
    return ema 

def create_insert_query(alert):
    """ Creates an insert sql statement for building the object and 
    a query for inserting it.
    """
    objectId =  alert['objectId']

    # Make a list of candidates and noncandidates in time order
    candlist = alert['prv_candidates'] + [alert['candidate']]
    ema = make_ema(candlist)

    ncand = 0
    ra = []
    dec = []
    magg = []
    magr = []
    jd   = []
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
        jd.append(cand['jd'])
        if cand['fid'] == 1:
            magg.append(cand['magpsf'])
            latestgmag = cand['magpsf']
        else:
            magr.append(cand['magpsf'])
            latestrmag = cand['magpsf']

        # if it also has the 'drb' data quality flag, copy the PS1 data
        if 'drb' in cand:
            sgmag1    = cand['sgmag1']
            srmag1    = cand['srmag1']
            sgscore1  = cand['sgscore1']
            distpsnr1 = cand['distpsnr1']
            if cand['drb'] > 0.75 and cand['isdiffpos'] == 't':
                ncandgp += 1
        ncand += 1

    # only want light curves with at least 2 candidates
    if ncand <= 1:
        return None

    # statistics of the g light curve
    if len(magg) > 0:
        maggmin = np.min(magg)
        maggmax = np.max(magg)
        maggmean = np.mean(magg)
        maggmedian = np.median(magg)
    else:
        maggmin = maggmax = maggmean = maggmedian = 'NULL'

    # statistics of the r light curve
    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        magrmedian = np.median(magr)
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
    sets['maggmedian'] = maggmedian
    sets['maggmean']   = maggmean
    sets['magrmin']    = magrmin
    sets['magrmax']    = magrmax
    sets['magrmedian'] = magrmedian
    sets['magrmean']   = magrmean
    sets['latestgmag'] = latestgmag
    sets['latestrmag'] = latestrmag
    sets['jdmin']      = np.min(jd)
    sets['jdmax']      = np.max(jd)
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
    sets['latest_dc_mag_g']   = ema['dc_mag_g']
    sets['latest_dc_mag_g02'] = ema['g02']
    sets['latest_dc_mag_g08'] = ema['g08']
    sets['latest_dc_mag_g28'] = ema['g28']
    sets['latest_dc_mag_r']   = ema['dc_mag_r']
    sets['latest_dc_mag_r02'] = ema['r02']
    sets['latest_dc_mag_r08'] = ema['r08']
    sets['latest_dc_mag_r28'] = ema['r28']

    # Sherlock conclusions
    sets['sherlock_classification']    = alert['sherlock_classification']
    sets['sherlock_annotation']        = alert['sherlock_annotation']
    sets['sherlock_summary']           = alert['sherlock_summary']
    sets['sherlock_separation_arcsec'] = alert['sherlock_separation_arcsec']

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

def create_insert_match(objectId, match):
    """ This code makes the insert query for the Sherlock crossmatch record
    """
    # Here are all the attributes that the database knows about
    attrs = [
        'catalogue_object_id', 'catalogue_table_id', 
        'separationArcsec', 'northSeparationArcsec', 'eastSeparationArcsec', 
        'id', 'z', 'scale', 'distance', 'distance_modulus', 'photoZ', 'photoZErr', 
        'association_type', 'physical_separation_kpc', 
        'catalogue_object_type', 'catalogue_object_subtype', 'association_rank', 
        'catalogue_table_name', 'rank', 'rankScore', 'search_name', 'major_axis_arcsec', 
        'direct_distance', 'direct_distance_scale', 'direct_distance_modulus', 
        'raDeg', 'decDeg', 'original_search_radius_arcsec', 'catalogue_view_id', 
        '_u', '_uErr', '_g', '_gErr', '_r', '_rErr', '_i', '_iErr', '_z', '_zErr', '_y', '_yErr', 
        'U', 'UErr', 'B', 'BErr', 'V', 'VErr', 'R', 'RErr', 'I', 'IErr', 
        'J', 'JErr', 'H', 'HErr', 'K', 'KErr', 'G', 'GErr', 
        'classificationReliability', 'transientAbsMag', 'merged_rank'
    ]

    # Put it in the list if we know about it
    sets = {}
    for key, value in match.items():
        if key in attrs:
            sets[key] = value

    # Build the query
    list = []
    query = 'REPLACE INTO sherlock_crossmatches SET objectId="%s",' % objectId
    for key,value in sets.items():
        if isinstance(value, str):
            list.append(key + '=' + '"' + str(value) + '"')
        else:
            list.append(key + '=' + str(value))
    query += ', '.join(list)
    query = query.replace('None', 'NULL')
    return query

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
