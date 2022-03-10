#!/usr/bin/env python
"""Get the TNS list, either each daily update or the whole thing.

Usage:
  %s [--daysAgo=<n>]
  %s [--radius=3]
  %s (-h | --help)
  %s (-v | --version)

Options:
  -h --help            Show this screen.
  --daysAgo=<n>        Which nightly report to fetch. 1 day ago is default.
                       If 'All', then the whole TNS database is scrubbed and rebuilt
  --radius=<f>         Matching radius, arcseconds, default 3
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, sys
import mysql.connector as MySQLdb
import csv
from datetime import datetime
from gkutils import Struct, dbConnect, cleanOptions
from gkhtm import _gkhtm as htmCircle
import tns_crossmatch
from fetch_from_tns import fetch_csv
import settings
sys.path.append('/home/ubuntu/lasair-lsst/utility')
from manage_status import manage_status

def getTNSRow(conn, tnsName):
   """
   Has the TNS row been updated compared with what's in the database?
   If so, return the details.
   """

   try:
      cursor = conn.cursor (dictionary=True)

      cursor.execute ("""
           select tns_prefix, tns_name from crossmatch_tns
            where tns_name = %s
      """, (tnsName,))
      resultSet = cursor.fetchone ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("ERROR in services/TNS: cannot connect to master database, Error %d: %s\n" % (e.args[0], e.args[1]))
      sys.stdout.flush()
      sys.exit (1)

   return resultSet

def countTNSRow(conn):
    """
    Computes number of sources in our copy of the TNS database.
    """
    try:
        cursor = conn.cursor (dictionary=True)
        cursor.execute ("select count(*) as nrow from crossmatch_tns")
        for row in cursor:
            nrow = row['nrow']
        cursor.close ()
        return nrow

    except MySQLdb.Error as e:
        print("Error %d: %s\n" % (e.args[0], e.args[1]))
        return -1

def deleteTNSRow(conn, tnsName):
    """
    Deletes a row from our copy of the TNS database
    """
    try:
        cursor = conn.cursor (dictionary=True)

        cursor.execute ("""
            delete from crossmatch_tns where tns_name = %s
            """, (tnsName,))

    except MySQLdb.Error as e:
        print("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return

def insertTNS(conn, tnsEntry):
    """
    Inserts a single row into our copy of the TNS database
    """
    e = {}
    for k,v in tnsEntry.items():
        # if its null, want the word NULL instead of ''
        if k == 'discoverymag'     and len(v) == 0:
            e[k] = 'NULL'

        # if its null, want the word NULL instead of ''
        elif k == 'redshift'       and len(v) == 0:
            e[k] = 'NULL'

        # just keep the first 75 characters of this,
        # and convert the unicode to ???
        elif k == 'internal_names' and len(v) > 75:
            e[k] = "'" + v[:75].replace("'", '') + "...'"
            e[k] = e[k].encode('ascii', 'replace').decode('ascii')

        # keep it down to 16 characters
        elif k == 'reporting_group':
            if len(v) > 12: v = v[:12] + "..."
            e[k] = "'" + v + "'"

        # just keep the first 75 characters of this,
        # and convert the unicode to ???
        elif k == 'reporters':
            v = v.encode('ascii', 'replace').decode('ascii').replace("'", '')
            if len(v) > 75: v = v[:75] + "..."
            e[k] = "'" + v + "'"

        # anything else, enclose in quotes
        else:
            e[k] = "'" + str(v) + "'"

    try:
        cursor = conn.cursor (dictionary=True)

# Can add these in when TNS provides them
#       hostz,
#       host_name,
#       ext_catalogs,

# This section exposes the names that we have for attributes
        query = """
        REPLACE INTO crossmatch_tns (
           ra,
           decl,
           tns_name,
           tns_prefix,
           disc_mag,
           disc_mag_filter,
           type,
           z,
           disc_int_name,
           disc_date,
           lastmodified_date,
           sender,
           reporters,
           source_group,
           htm16)
        VALUES (%s,%s,%s,%s,%s,  %s,%s,%s,%s,%s,  %s,%s,%s,%s,%s )
        """

# This section exposes the names that TNS has for attributes
        query = query % (
            e['ra'],
            e['declination'],
            e['name'],
            e['name_prefix'],
            e['discoverymag'],
            e['filter'],
            e['type'],
            e['redshift'],
            e['internal_names'],
            e['discoverydate'],
            e['lastmodified'],
            e['reporting_group'],
            e['reporters'],
            e['source_group'],
            e['htm16'])

#        print(query)
        cursor.execute (query)
        insertId = cursor.lastrowid
        cursor.close ()

    except MySQLdb.Error as e:
        print('ERROR in services/TNS/poll_tns', e)
        print(tnsEntry)
        print(e)
        print(query)
        sys.stdout.flush()

def getTNSData(opts, conn):
    """
    Fetch CSV file from TNS, either the daily update (daysAgo=1) 
    or the whole thing (daysAgo=All).
    """
    from datetime import datetime, date, time, timedelta
    if type(opts) is dict:
        options = Struct(**opts)
    else:
        options = opts

    radius = 3.0 # arcseconds from crossmatch
    if options.radius:
        radius = float(options.radius)

    if options.daysAgo == 'All':
        doingAll = True
        # truncate the cables crossmatch_tns, and
        #     watchlist_cones(TNS), watchlist_hits(TNS)
        truncate_tns(conn)

        # get the data file from TNS
        data = fetch_csv('All')

#        data = data[:10]   reduce to 10 for testing
    else:
        doingAll = False
        try:
            daysAgo = int(options.daysAgo)
        except:
            daysAgo = 1
            
        if daysAgo <= 0:
            print('ERROR in services/TNS/poll_tns: daysAgo must be >+1 or "All"')
            return
        pastTime = datetime.now() - timedelta(days=daysAgo)
        pastTime = pastTime.strftime("%Y%m%d")

        # get the data file from TNS
        data = fetch_csv(pastTime)

    # First row of the CSV is the header names
    header = data[0]
    rowsAdded = 0
    rowsChanged = 0

    for row in data[1:]:
        row_dict = {}
        for i in range(len(header)):
            row_dict[header[i]] = row[i]

        prefix =    row_dict['name_prefix']
        name =      row_dict['name']
        ra  = float(row_dict['ra'])
        dec = float(row_dict['declination'])

        # Compute the HTM
        htm16 = htmCircle.htmID(16, ra, dec)
        row_dict['htm16'] = htm16

        if not doingAll:
            tnsEntry = getTNSRow(conn, name)
        else:
            tnsEntry = None  # No point checking if we jus truncated the table

        if tnsEntry:
            if tnsEntry['tns_prefix'] != prefix:
                # The entry has been updated on TNS - classified! Otherwise do nothing!
                deleteTNSRow(conn, name)
                insertTNS(conn, row_dict)
                print("Object %s has been updated" % row_dict['name'])
                rowsChanged += 1
        else:
            insertTNS(conn, row_dict)
            tns_crossmatch.tns_name_crossmatch(\
                    conn, row_dict['name'], ra, dec, radius)
            rowsAdded += 1

            if doingAll:
                if rowsAdded % 1000 == 0:
                    print(rowsAdded)
            else:
                print("Object %s has been added" % row_dict['name'])

#        print(prefix, name, ra, dec, htm16)

    print("Total rows added = %d, modified = %d\n" % (rowsAdded, rowsChanged))

def truncate_tns(conn):
    """ Delete all the cones, hits, and crossmatch_tns 
    """
    cursor  = conn.cursor(buffered=True, dictionary=True)
    query = 'DELETE FROM watchlist_cones WHERE wl_id=%d' % settings.TNS_WATCHLIST_ID
    cursor.execute(query)

    query = 'DELETE FROM watchlist_hits WHERE wl_id=%d' % settings.TNS_WATCHLIST_ID
    cursor.execute(query)
    conn.commit()

    query = 'TRUNCATE crossmatch_tns'
    cursor.execute(query)
    conn.commit()

def get_db():
    username = settings.DB_USER_WRITE
    password = settings.DB_PASS_WRITE
    hostname = settings.DB_HOST
    port     = settings.DB_PORT
    database = 'ztf'
    conn = dbConnect(hostname, username, password, database, lport=port)
    if not conn:
        print("ERROR in services/TNS/poll_tns: Cannot connect to the database\n")
        return 1
    return conn

if __name__ == '__main__':
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    conn = get_db()
    options = Struct(**opts)

    getTNSData(options, conn)

    countTNS = countTNSRow(conn)
    ms = manage_status(settings.SYSTEM_STATUS)
    nid = date_nid.nid_now()
    ms.set({'countTNS':countTNS}, nid)

    conn.commit()
    conn.close()
