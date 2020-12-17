import sys
import math
import settings

from gkhtm import _gkhtm as htmCircle

def distance(ra1, de1, ra2, de2):
    dra = (ra1 - ra2)*math.cos(de1*math.pi/180)
    dde = (de1 - de2)
    return math.sqrt(dra*dra + dde*dde)

# setup database connection
import mysql.connector
config = {
    'user'    : settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

def run_tns_crossmatch(radius):
    cursor  = msl.cursor(buffered=True, dictionary=True)
    n_tns = 0
    n_hits = 0
    n_newhits = 0
    # get all the cones and run them
    query = 'SELECT tns_name, ra,decl FROM crossmatch_tns'
    cursor.execute(query)
    for row in cursor:
        n_tns += 1
        n_hits += tns_name_crossmatch(row['tns_name'], row['ra'], row['decl'], radius)
    print("%d entries in TNS, %d hits in ZTF" % (n_tns, n_hits))
    return

def tns_name_crossmatch(tns_name, myRA, myDecl, radius, logfile=None):
    cursor2 = msl.cursor(buffered=True, dictionary=True)
# insert the new TNS entry into the watchlist
    query2 = 'INSERT INTO watchlist_cones (wl_id, name, ra, decl) VALUES (%d, "%s", %f, %f)'
    query2 = query2 % (settings.TNS_WATCHLIST_ID, tns_name, myRA, myDecl)
    cursor2.execute(query2)

    query2 = 'SELECT LAST_INSERT_ID() AS cone_id'
    cursor2.execute(query2)
    for row in cursor2:
        cone_id = row['cone_id']

    s = '%s got cone_id %d' % (tns_name, cone_id)
    if logfile: logfile.write(s)
    else:       print(s)

# refresh the watchlist so ist MOC file is remade
    query2 = 'UPDATE watchlists SET timestamp=NOW() WHERE wl_id=%d' % settings.TNS_WATCHLIST_ID
    cursor2.execute(query2)

    cursor3 = msl.cursor(buffered=True, dictionary=True)
    subClause = htmCircle.htmCircleRegion(16, myRA, myDecl, radius)
    subClause = subClause.replace('htm16ID', 'htm16')
    query2 = 'SELECT * FROM objects WHERE htm16 ' + subClause[14: -2]
#    print(query2)
    cursor2.execute(query2)
    n_hits = 0
    for row in cursor2:
        objectId = row['objectId']
        arcsec = 3600*distance(myRA, myDecl, row['ramean'], row['decmean'])
        if arcsec > radius:
            continue
        n_hits += 1
        query3 = "REPLACE into watchlist_hits (wl_id, cone_id, objectId, arcsec, name) VALUES\n"
        query3 += ' (%d, %d, "%s", %.2f, "%s")' % (settings.TNS_WATCHLIST_ID, cone_id, objectId, arcsec, tns_name)
        cursor3.execute(query3)
        msl.commit()

        s = '%s matches %s' % (tns_name, objectId)
        if logfile: logfile.write(s)
        else:       print(s)
    return n_hits

if __name__ == "__main__":
    radius = 3  # arcseconds
    run_tns_crossmatch(radius)
