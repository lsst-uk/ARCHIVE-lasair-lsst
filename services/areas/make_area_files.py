"""
make_area.py
This code uses area information in the database to build and cache files 
that can be used for area determination against alerts. The files are 
named ar_<nn>.fits where nn is the area id from the database. These files are 
"Multi-Order Coverage maps", https://cds-astro.github.io/mocpy/. 
"""
import mysql.connector
import os, sys
import stat
import time
from datetime import datetime
import base64
sys.path.append('/home/ubuntu/lasair-lsst/utility')
import date_nid

logfile = ''

def bytes2string(bytes):
    """bytes2string.

    Args:
        bytes:
    """
    base64_bytes   = base64.b64encode(bytes)
    str = base64_bytes.decode('utf-8')
    return str

def string2bytes(str):
    """string2bytes.

    Args:
        str:
    """
    base64_bytes  = str.encode('utf-8')
    bytes = base64.decodebytes(base64_bytes)
    return bytes

def write_cache_file(msl, ar_id, cache_dir):
    """
    Fetch the area from the database. 
    """
    cursor = msl.cursor(buffered=True, dictionary=True)

    cursor.execute('SELECT name,moc FROM areas WHERE ar_id=%d ' % ar_id)
    # Build lists of all the data from the database
    for row in cursor:
        txtmoc = row['moc']
        logfile.write('caching area %s\n' % row['name'])
    moc = string2bytes(txtmoc)

    area_file = cache_dir + '/ar_%d.fits' % ar_id
    f = open(area_file, 'wb')
    f.write(moc)
    f.close()

def fetch_active_areas(msl, cache_dir):
    """
    Go through the database and fetch the active areas
    Select those fresher than their cache and rebuild their cache.
    """
    cursor = msl.cursor(buffered=True, dictionary=True)

    keep = []
    get  = []
    cursor.execute('SELECT ar_id, name, timestamp FROM areas WHERE active > 0 ')
    for row in cursor:
        # unix time of last update from the database
        area_timestamp = time.mktime(row['timestamp'].timetuple())

        # directory where the cache files are kept
        area_file = cache_dir + '/ar_%d.fits'%row['ar_id']

        try:
            # unix time of last modification of this directory
            dir_timestamp = os.stat(area_file)[ stat.ST_MTIME ] 
        except:
            dir_timestamp = 0
        newer = area_timestamp - dir_timestamp

        # if the area from the database is newer than the cache, rebuild it
        if newer > 0:
            get.append(row['ar_id'])
        else:
            keep.append(row['ar_id'])
    # areas which will have their caches rebuilt
    return {'keep': keep, 'get':get}

if __name__ == "__main__":
    import settings
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    logfile = open('/mnt/cephfs/roy/services_log/' + date + '.log', 'a')
    now = datetime.now()
    logfile.write('\n-- make_area_files at %s\n' % now.strftime("%d/%m/%Y %H:%M:%S"))

    msl = mysql.connector.connect(
        user    =settings.DB_USER_READ,
        password=settings.DB_PASS_READ,
        host    =settings.DB_HOST,
        database='ztf'
    )

    cache_dir = settings.AREA_MOCS
    new_cache_dir = cache_dir + '_new'
    os.system('mkdir %s' % new_cache_dir)

    # who needs to be recomputed
    areas = fetch_active_areas(msl, cache_dir)

    for ar_id in areas['keep']:
        os.system('mv %s/ar_%d.fits %s' % (cache_dir, ar_id, new_cache_dir))
    for ar_id in areas['get']:
        write_cache_file(msl, ar_id, new_cache_dir)
    os.system('rm -r %s'  % (cache_dir))
    os.system('mv %s %s' % (new_cache_dir, cache_dir))
