"""Consumes stream for ingesting to database
"""

from __future__ import print_function
from mag import dc_mag_dict
import json
import math
import numpy as np
import ephem

def make_ema(candlist):
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
        dc_mag = d['dc_mag']
        print(dc_mag)

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
    """ Creates an insert sql statement for insering the canditate info
        Also works foe candidates in the prv
    """
    objectId =  alert['objectId']
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
        if cand['candid'] is None:
            continue
        ra.append(cand['ra'])
        dec.append(cand['dec'])
        jd.append(cand['jd'])
        if cand['fid'] == 1:
            magg.append(cand['magpsf'])
            latestgmag = cand['magpsf']
        else:
            magr.append(cand['magpsf'])
            latestrmag = cand['magpsf']

        try:
            sgmag1    = cand['sgmag1']
            srmag1    = cand['srmag1']
        except:
            pass

        try:
            if(cand['sgscore1']):  sgscore1  = cand['sgscore1']
            if(cand['distpsnr1']): distpsnr1 = cand['distpsnr1']
        except:
            pass

        ncand += 1
        try:
            if cand['drb'] and cand['drb'] > 0.75 and cand['isdiffpos'] == 't':
                ncandgp += 1
        except:
            pass

    if len(magg) > 0:
        maggmin = np.min(magg)
        maggmax = np.max(magg)
        maggmean = np.mean(magg)
        maggmedian = np.median(magg)
    else:
        maggmin = maggmax = maggmean = maggmedian = 'NULL'

    if len(magr) > 0:
        magrmin = np.min(magr)
        magrmax = np.max(magr)
        magrmean = np.mean(magr)
        magrmedian = np.median(magr)
    else:
        magrmin = magrmax = magrmean = magrmedian = 'NULL'

    ramean  = np.mean(ra)
    decmean = np.mean(dec)
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

    sets = {}
    sets['ncand']      = ncand
    sets['stale']      = 0
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
    sets['sgmag1']     = sgmag1
    sets['srmag1']     = srmag1
    sets['sgscore1']   = sgscore1
    sets['distpsnr1']  = distpsnr1
    sets['ncandgp']    = ncandgp
    sets['htm16']      = htm16

    sets['latest_dc_mag_g']   = ema['dc_mag_g']
    sets['latest_dc_mag_g02'] = ema['g02']
    sets['latest_dc_mag_g08'] = ema['g08']
    sets['latest_dc_mag_g28'] = ema['g28']

    sets['latest_dc_mag_r']   = ema['dc_mag_r']
    sets['latest_dc_mag_r02'] = ema['r02']
    sets['latest_dc_mag_r08'] = ema['r08']
    sets['latest_dc_mag_r28'] = ema['r28']

    list = []
    query = 'UPDATE objects SET '
    for key,value in sets.items():
        list.append(key + '=' + str(value))
    query += ', '.join(list)
    query += ' WHERE objectId="' + objectId + '"'
    query = query.replace('None', 'NULL')
    print ('%s with %d candidates' % (objectId, ncand))
    return query

import os
if __name__ == '__main__':
    root = '/mnt/cephfs/roy/lightcurve/'
    for hex3 in os.listdir(root):
        for jsonfile in os.listdir(root + hex3):
            filename = root + hex3 + '/' + jsonfile
            print(filename)
            alert = json.loads(open(filename).read()) 
            query = create_insert_query(alert)
