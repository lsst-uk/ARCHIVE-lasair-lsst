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
from mocpy import MOC
import astropy.units as u

def read_watchlist_cache_files(cache_dir):
    """
    This function reads all the files in the cache directories and keeps them in memory
    in a list called "watchlistlist". Each watchlist is a dictionary:
        cone_ids: the integer ids of the watchlist cones
        ra, de: the lists of ra and dec posiitons of the cones, in degrees
        radius: the list of radii of the cones about those points
        names: the naems give tot he cones by the user
    """
    watchlistlist = []
    for wl_dir in os.listdir(cache_dir):
        # every directory in the cache should be of the form wl_<nn> 
        # where nn is the watchlist id
        try:     wl_id = int(wl_dir[3:])
        except:  continue

        # id of the watchlist
        watchlist = {'wl_id':wl_id}

        moclist = []
        filelist = os.listdir(cache_dir +'/'+  wl_dir)
        filelist.sort()
        for file in filelist:
            gfile = cache_dir +'/'+ wl_dir + '/' + file

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
    watchralist   = cones['ra']
    watchdelist   = cones['de']
    watchradius   = cones['radius']

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
                        'cone_id' :cones['cone_ids'][iw],
                        'wl_id'   :wl_id, 
                        'objectId':objectId,
                        'name'    :cones['names'][iw],
                        'arcsec'  : d*3600
                    })

    return hits

def check_alerts_against_watchlist(alertlist, watchlist, chk):
    """ This function goes through all the watchlists looking for hits
    """
    moclist = watchlist['moclist']
    cones   = watchlist['cones']
    wl_id   = watchlist['wl_id']
    hits = []
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

def check_alerts_against_watchlists(alertlist, watchlistlist, chunk_size):
    """ check the batch of alerts agains all the watchlists
    """
    hits = []
    for watchlist in watchlistlist:
#        print(watchlist['wl_id'])
        hits += check_alerts_against_watchlist(alertlist, watchlist, chunk_size)
    return hits

def fetch_alerts(msl):
    """ Get all the alerts from the local cache to check againstr watchlist
    """
    cursor = msl.cursor(buffered=True, dictionary=True)

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

def get_watchlist_hits(msl, cache_dir, chunk_size):
    """ Get all the alerts, then run against the watchlists, return the hits
    """
    # read in the cache files
    watchlistlist = read_watchlist_cache_files(cache_dir)

    # get the alert positions from the database
    alertlist = fetch_alerts(msl)

    # check the list against the watchlists
    hits = check_alerts_against_watchlists(alertlist, watchlistlist, chunk_size)
    return hits

def insert_watchlist_hits(msl, hits):
    """ Build and execute the insertion query to get the hits into the database
    """
    print('inserting')
    cursor = msl.cursor(buffered=True, dictionary=True)

    query = "INSERT into watchlist_hits (wl_id, cone_id, objectId, arcsec, name) VALUES\n"
    list = []
    for hit in hits:
        list.append('(%d,%d,"%s",%.3f,"%s")' %  \
            (hit['wl_id'], hit['cone_id'], hit['objectId'], hit['arcsec'], hit['name']))
    query += ',\n'.join(list)
    try:
       cursor.execute(query)
       cursor.close()
    except mysql.connector.Error as err:
       print('WATCHLIST object Database insert candidate failed: %s' % str(err))
    msl.commit()

if __name__ == "__main__":
    import settings
    import mysql.connector
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    msl_local = mysql.connector.connect(**config)

    # can run the watchlist process without the rest of the filter code 
    hits = get_watchlist_hits(msl_local, settings.WATCHLIST_MOCS, settings.WATCHLIST_CHUNK)
    if hits:
        for hit in hits: print(hit)
        insert_watchlist_hits(msl_local, hits)
