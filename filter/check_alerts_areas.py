"""
check_alerts_areas.py
This code checks a batch of alerts against the cached area files, The files are kept 
in a file named ar_<nn>.fits where nn is the area id from the database. 
The "moc<nnn>.fits" files are
"Multi-Order Coverage maps", https://cds-astro.github.io/mocpy/. 
"""
import os
import math
from mocpy import MOC
import astropy.units as u

def read_area_cache_files(cache_dir):
    """
    This function reads all the files in the cache directories and keeps them in memory
    in a list called "arealist". Each area is a dictionary:
        ar_id: the id from tha database
        moc  : the ingestred moc
    """
    arealist = []
    for ar_file in os.listdir(cache_dir):
        # every file in the cache should be of the form ar_<nn>.fits
        # where nn is the area id
        tok = ar_file.split('.')
        if tok[1] != 'fits': continue
        try:     ar_id = int(tok[0][3:])
        except:  continue

        gfile = cache_dir + '/' + ar_file
        moc = MOC.from_fits(gfile)
        area = {'ar_id':ar_id, 'moc':moc}
        arealist.append(area)
    return arealist

def check_alerts_against_area(alertlist, area):
    """ For a given moc, check the alerts in the batch 
    """
    # alert positions
    alertobjlist = alertlist['obj']
    alertralist  = alertlist['ra']
    alertdelist  = alertlist['de']

    # here is the crossmatch
    result = area['moc'].contains(alertralist*u.deg, alertdelist*u.deg)

    hits = []
    # go through the boolean vector, looking for hits
    for ialert in range(len(alertralist)):
        if(result[ialert]):
            hits.append({
                        'ar_id'   :area['ar_id'], 
                        'objectId':alertobjlist[ialert]
                    })
    return hits

def check_alerts_against_areas(alertlist, arealist):
    """ check the batch of alerts agains all the areas
    """
    hits = []
    for area in arealist:
        ar_id = area['ar_id']
        hits += check_alerts_against_area(alertlist, area)
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

def get_area_hits(msl, cache_dir):
    """ Get all the alerts, then run against the arealist, return the hits
    """
    # read in the cache files
    arealist = read_area_cache_files(cache_dir)

    # get the alert positions from the database
    alertlist = fetch_alerts(msl)

    # check the list against the watchlists
    hits = check_alerts_against_areas(alertlist, arealist)
    return hits

def insert_area_hits(msl, hits):
    """ Build and execute the insertion query to get the hits into the database
    """
    print('inserting')
    cursor = msl.cursor(buffered=True, dictionary=True)

    query = "INSERT into area_hits (ar_id, objectId) VALUES\n"
    list = []
    for hit in hits:
        list.append('(%d,"%s")' %  (hit['ar_id'], hit['objectId']))
    query += ',\n'.join(list)
    try:
       cursor.execute(query)
       cursor.close()
    except mysql.connector.Error as err:
       print('AREA object Database insert candidate failed: %s' % str(err))
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

    # can run the area process without the rest of the filter code 
    hits = get_area_hits(msl_local, settings.AREA_MOCS)
    if hits:
        for hit in hits: print(hit)
        insert_area_hits(msl_local, hits)
