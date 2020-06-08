"""
check_alerts_watchlists.py
This code checks a batch of alerts against the cached watchlist files,
that can be used for very fast crossmatch against alerts. The files are kept in
a directory named wl_<nn> where nn is the watchlist id from the database. Inside:
     watchlist.csv
     moc000.fits
     moc001.fits
     ...
where the first has the columns
    cone_id, ra, dec, radius, name
where cone_id is the id of the cone in the database, at the given position and radius
(in degrees), with the user-given name of the cone last. The "moc<nnn>.fits" files are
"Multi-Order Coverage maps", https://cds-astro.github.io/mocpy/. The union of all the
files is the same as the list of cones associated with the watchlist.
"""
import os
import math
import settings
from mocpy import MOC
import astropy.units as u
import mysql.connector

def read_watchlist_cache_files():
    """
    This function reads all the files in the cache directories and keeps them in memory
    in a list called "watchlistlist". Each watchlist is a dictionary:
        cone_ids: the integer ids of the watchlist cones
        ra, de: the lists of ra and dec posiitons of the cones, in degrees
        radius: the list of radii of the cones about those points
        names: the naems give tot he cones by the user
    """
    watchlistlist = []
    for wl_dir in os.listdir(settings.WATCHLIST_MOCS):
        # every directory in the cache should be of the form wl_<nn> 
        # where nn is the watchlist id
        try:     wl_id = int(wl_dir[3:])
        except:  continue

        # id of the watchlist
        watchlist = {'wl_id':wl_id}

        moclist = []
        for file in os.listdir(settings.WATCHLIST_MOCS + wl_dir):
            gfile = settings.WATCHLIST_MOCS + wl_dir + '/' + file

            # read in the mocs
            if file.startswith('moc'):
                moclist.append(MOC.from_fits(gfile))

            # read in the csv files of watchlist cones
            if file.startswith('watchlist'):
                cone_ids = []
                ralist   = []
                delist   = []
                radius   = []
                names    = []
                for line in open(gfile).readlines():
                    tok = line.split(',')
                    cone_ids.append(int(tok[0]))
                    ralist.append(float(tok[1]))
                    delist.append(float(tok[2]))
                    radius.append(float(tok[3]))
                    names.append(tok[4].strip())
                watchlist['cones'] = {
                    'cone_ids':cone_ids, 
                    'ra':ralist, 'de':delist, 
                    'radius':radius, 
                    'names':names
                }
            watchlist['moclist'] = moclist

        watchlistlist.append(watchlist)
    return watchlistlist

def check_alerts_against_moc(alertlist, wl_id, moc, cones):
    """ For a given moc, check the alerts in the batch 
    """
    # alert positions
    alertobjlist = alertlist['obj']
    alertralist  = alertlist['ra']
    alertdelist  = alertlist['de']

    # watchlist cones
    watchralist = cones['ra']
    watchdelist = cones['de']
    watchradius = cones['radius']

    # here is the crossmatch
    result = moc.contains(alertralist*u.deg, alertdelist*u.deg)

    hits = []
    # go through the boolean vector, looking for hits
    for ialert in range(len(alertralist)):
        if(result[ialert]):
            # when there is a hit, we need to know *which* cone contains the alert
            objectId = alertobjlist[ialert]
            ra       = alertralist[ialert]
            de       = alertdelist[ialert]
            for iw in range(len(watchralist)):
                # don't forget the loxodrome
                dra = (ra - watchralist[iw])*math.cos(de*math.pi/180)
                dde = (de - watchdelist[iw])
                # dra and dde are angular great circle distance
                d = math.sqrt(dra*dra + dde*dde)
                if d < watchradius[iw]:
                    # got a real hit -- record the crossmatch
                    hits.append({
                        'ialert'  :ialert,
                        'wl_id'   :wl_id, 
                        'objectId':objectId,
                        'name'    :cones['names'][iw],
                        'arcsec'  : d*3600
                    })

    return hits

def check_alerts_against_watchlist(alertlist, watchlist):
    """ This function goes through all the watchlists looking for hits
    """
    moclist = watchlist['moclist']
    cones   = watchlist['cones']
    wl_id   = watchlist['wl_id']
    hits = []
    chk = settings.WATCHLIST_CHUNK
    # larger watchlists are expressed by multiple mocs
    for ichunk in range(len(moclist)):
        coneschunk  = {
            'cone_ids':cones['cone_ids'][ichunk*chk:(ichunk+1)*chk],
            'ra'      :cones['ra']      [ichunk*chk:(ichunk+1)*chk],
            'de'      :cones['de']      [ichunk*chk:(ichunk+1)*chk],
            'radius'  :cones['radius']  [ichunk*chk:(ichunk+1)*chk],
            'names'   :cones['names']   [ichunk*chk:(ichunk+1)*chk],
        }
        hits += check_alerts_against_moc(alertlist, wl_id, moclist[ichunk], coneschunk)
    return hits

def check_alerts_against_watchlists(alertlist, watchlistlist):
    """ check the batch of alerts agains all the watchlists
    """
    hits = []
    for watchlist in watchlistlist:
        hits += check_alerts_against_watchlist(alertlist, watchlist)
    return hits

def fetch_alerts():
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    msl_local = mysql.connector.connect(**config)
    cursor = msl_local.cursor(buffered=True, dictionary=True)
    query = 'SELECT objectId, ramean, decmean from objects'
    cursor.execute(query)
    objlist = []
    ralist = []
    delist = []
    for row in cursor:
        objlist.append(row['objectId'])
        ralist.append (row['ramean'])
        delist.append (row['decmean'])
    return {"obj":objlist, "ra":ralist, "de":delist}

def get_watchlist_hits():
    # read in the cache files
    watchlistlist = read_watchlist_cache_files()

    # get the alert positions from the database
    alertlist = fetch_alerts()

    # check the list against the watchlists
    hits = check_alerts_against_watchlists(alertlist, watchlistlist)

if __name__ == "__main__":
    hits = get_watchlist_hits()
    for hit in hits: print(hit)
