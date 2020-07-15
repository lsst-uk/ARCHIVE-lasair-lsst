"""
make_watchlist_files.py
This code uses watchlist information in the database to build and cache files 
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
from mocpy import MOC
import astropy.units as u
import mysql.connector
import math
import time
import os
import stat

def moc_watchlist(watchlist, max_depth):
    """
    Take a "watchlist" dictionary and builds a MOC at given max_depth, by approximating the
    disk around a source as a hexagon.
    """
    s = 0.5*math.sqrt(3)
    moc = None
    ralist = watchlist['ra']      # ra list of the watchlist, degrees
    delist = watchlist['de']      # dec list of the watchlist, degrees
    radius = watchlist['radius']  # radius list in degrees
    for i in range(len(ralist)):
        ra = ralist[i]
        de = delist[i]
        q = radius[i]
        r = q/math.cos(de*math.pi/180)

        # make a hexagon
        lon = [ra+q, ra+0.5*r, ra-0.5*r, ra-r, ra-0.5*r, ra+0.5*r] * u.deg
        lat = [de,     de+s*q,  de+s*q,   de,   de-s*q,   de-s*q] * u.deg

        # make a moc from the hexagon
        newmoc = MOC.from_polygon(lon, lat, max_depth=max_depth)

        # union with previous hexagons
        if moc: moc = moc.union(newmoc)
        else:   moc = newmoc
    return moc

def moc_watchlists(watchlist, max_depth):
    """
    mocs can be inefficient when they have a lot of points in them
    so here we make several smaller mocs, each with a max number of cones
    """
    chk = settings.WATCHLIST_CHUNK   # max points per moc, perhaps 50,000

    # split the number of points in the watchlist into chunks
    nchunk = len(watchlist['ra'])//chk
    if len(watchlist['ra']) % chk: 
        nchunk += 1

    # split up the watchlist and make a moc for each chunk
    moclist = []
    for ichunk in range(nchunk):
        watchchunk  = {
            'ra':     watchlist['ra']    [ichunk*chk:(ichunk+1)*chk],
            'de':     watchlist['de']    [ichunk*chk:(ichunk+1)*chk],
            'radius': watchlist['radius'][ichunk*chk:(ichunk+1)*chk]
        }
        moclist.append(moc_watchlist(watchchunk, max_depth))
    # return a list of mocs
    return moclist


def fetch_watchlist(msl, wl_id, default_radius):
    """
    Fetch the watchlist from the database. If radius is not supplied per cone, 
    then use the default radius of the watchlist
    """
    cursor = msl.cursor(buffered=True, dictionary=True)

    cursor.execute('SELECT cone_id, ra, decl, radius, name FROM watchlist_cones WHERE wl_id=%d ' % wl_id)
    cone_ids = []
    ralist   = []
    delist   = []
    radius   = []
    names    = []
    ncone = 0
    # Build lists of all the data from the database
    for row in cursor:
        r = default_radius
        if row['radius'] is not None: 
            r = row['radius']
        cone_ids.append(row['cone_id'])
        ralist  .append(row['ra'])
        delist  .append(row['decl'])
        radius  .append(r/3600.0)
        names   .append(row['name'])
        ncone += 1
#    print('wl_id=%d has %d cones' % (wl_id, ncone))
    return {'cone_ids':cone_ids, 'ra':ralist, 'de':delist, 'radius':radius, 'names':names}

def fetch_active_watchlists(msl, cache_dir):
    """
    Go through the database and fetch the active watchlists
    Select those fresher than their cache and rebuild their cache.
    """
    cursor = msl.cursor(buffered=True, dictionary=True)

    watchlist_list = []
    cursor.execute('SELECT wl_id, name, radius, timestamp FROM watchlists WHERE active > 0 ')
    for row in cursor:
        # unix time of last update from the database
        watchlist_timestamp = time.mktime(row['timestamp'].timetuple())

        # directory where the cache files are kept
        watchlist_dir = cache_dir + 'wl_%d'%row['wl_id']

        try:
            # unix time of last modification of this directory
            dir_timestamp = os.stat(watchlist_dir)[ stat.ST_MTIME ] 
        except:
            dir_timestamp = 0
        newer = watchlist_timestamp - dir_timestamp

        # if the watchlist from the database is newer than the cache, rebuild it
        # print(row['wl_id'], 'watchlist newer by %d seconds'%newer)
        if newer > 0:
            watchlist_list.append({'wl_id':row['wl_id'], 'name':row['name'],'radius':row['radius']})
    # watchlists which will have their caches rebuilt
    return watchlist_list

def rebuild_cache(wl_id, name, cones, max_depth):
    t = time.time()
    # clear the cache and remake the directory
    watchlist_dir = cache_dir + 'wl_%d/' % wl_id
    try:    os.system('rm -r ' + watchlist_dir)
    except: pass 
    os.mkdir(watchlist_dir)

    # compute the list of mocs
    moclist = moc_watchlists(cones, max_depth)
    
    # write the watchlist.csv
    w = open(watchlist_dir + 'watchlist.csv', 'w')
    ralist   = cones['ra']
    delist   = cones['de']
    radius   = cones['radius']
    names    = cones['names']
    cone_ids = cones['cone_ids']
    for i in range(len(ralist)):
        w.write('%d, %f, %f, %.3e, %s\n' % 
            (cone_ids[i], ralist[i], delist[i], radius[i], names[i]))

    # now write the moc files
    for i in range(len(moclist)):
        moclist[i].write(watchlist_dir + 'moc%03d.fits'%i)
    print('Watchlist "%s" with %d cones rebuilt in %.2f seconds' 
            % (name, len(ralist), time.time() - t))

if __name__ == "__main__":
    import settings
    msl = mysql.connector.connect(
        user    =settings.DB_USER_REMOTE,
        password=settings.DB_PASS_REMOTE,
        host    =settings.DB_HOST_REMOTE,
        database='ztf'
    )

    max_depth = settings.WATCHLIST_MAX_DEPTH
    cache_dir = settings.WATCHLIST_MOCS

    # who needs to be recomputed
    watchlists = fetch_active_watchlists(msl, cache_dir)

    for watchlist in watchlists:
        # get the data from the database
        cones = fetch_watchlist(msl, watchlist['wl_id'], watchlist['radius'])
        rebuild_cache(watchlist['wl_id'], watchlist['name'], cones, max_depth)
