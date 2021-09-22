#!/usr/bin/env python
"""Get the TNS list.  Max size of request is 1000 rows.

Usage:
  %s [--pageSize=<n>] [--pageNumber=<n>] [--radius] [--inLastNumberOfDays=<n>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --pageSize=<n>               Request page size [default: 50].
  --pageNumber=<n>             Request page number [default: 0].
  --radius                     Radius in arcseconds for crossmatch
  --inLastNumberOfDays=<n>     Get everything in the specified number of days.

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
import mysql.connector as MySQLdb
from gkutils import Struct, cleanOptions, dbConnect, coords_sex_to_dec, floatValue, intValue, nullValue
import requests
import csv
from datetime import datetime
import run_tns_crossmatch
from gkhtm import _gkhtm as htmCircle

sys.path.append('/home/ubuntu/lasair-lsst/utility')
import date_nid
import settings

global logfile

def getTNSRow(conn, tnsName):
   """
   Has the TNS row been updated compared with what's in the database?
   """

   try:
      #cursor = conn.cursor (MySQLdb.cursors.DictCursor)
      cursor = conn.cursor (dictionary=True)

      cursor.execute ("""
           select tns_prefix, tns_name
             from crossmatch_tns
            where tns_name = %s
      """, (tnsName,))
      resultSet = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("ERROR in services/TNS: cannot connect to master database, Error %d: %s\n" % (e.args[0], e.args[1]))
      sys.stdout.flush()
      sys.exit (1)

   return resultSet


def deleteTNSRow(conn, tnsName):
    try:
        #cursor = conn.cursor (MySQLdb.cursors.DictCursor)
        cursor = conn.cursor (dictionary=True)

        cursor.execute ("""
            delete from crossmatch_tns
            where tns_name = %s
            """, (tnsName,))

    except MySQLdb.Error as e:
        logfile.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    conn.commit()
    return


def insertTNS(conn, tnsEntry):

    insertId = 0
    try:
        #cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor = conn.cursor (dictionary=True)

        cursor.execute ("""
            insert into crossmatch_tns (
               ra,
               decl,
               tns_prefix,
               tns_name,
               z,
               hostz,
               host_name,
               disc_mag,
               disc_mag_filter,
               disc_instruments,
               type,
               public,
               sender,
               associated_groups,
               classifying_groups,
               discovering_groups,
               class_instrument,
               tnsid,
               disc_int_name,
               ext_catalogs,
               disc_date,
               tns_at,
               htm16)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (tnsEntry['ra'],
                  tnsEntry['dec'],
                  tnsEntry['prefix'],
                  tnsEntry['suffix'],
                  floatValue(tnsEntry['Redshift']),
                  floatValue(tnsEntry['Host Redshift']),
                  nullValue(tnsEntry['Host Name']),
                  floatValue(tnsEntry['Discovery Mag/Flux']),
                  nullValue(tnsEntry['Discovery Filter']),
                  nullValue(tnsEntry['Disc. Instrument/s']),
                  nullValue(tnsEntry['Obj. Type']),
                  intValue(tnsEntry['Public']),
                  nullValue(tnsEntry['Sender']),
                  nullValue(tnsEntry['Associated Group/s']),
                  nullValue(tnsEntry['Classifying Group/s']),
                  nullValue(tnsEntry['Reporting Group/s']),
                  nullValue(tnsEntry['Class. Instrument/s']),
                  intValue(tnsEntry['ID']),
                  nullValue(tnsEntry['Disc. Internal Name']),
                  nullValue(tnsEntry['Ext. catalog/s']),
                  nullValue(tnsEntry['Discovery Date (UT)']),
                  intValue(tnsEntry['TNS AT']),
                  tnsEntry['htm16']))
        insertId = cursor.lastrowid
        cursor.close ()

    except MySQLdb.Error as e:
        if e[0] == 1142: # Can't insert - don't have permission
            print("ERROR in services/TNS/poll_tns: Can't insert.  User doesn't have permission.\n")
        else:
            print('ERROR in services/TNS/poll_tns', e)
        sys.stdout.flush()

    #insertId = conn.insert_id()
    conn.commit()
    return insertId


def pollTNS(page=0, resultSize=50, inLastNumberDays=None):

    from datetime import datetime, date, time, timedelta
    if inLastNumberDays:
        pastTime = datetime.now() - timedelta(days=inLastNumberDays)
        pastTime = pastTime.strftime("%Y-%m-%d")
    else:
        pastTime = ""

    try:
        tns_user_agent = 'tns_marker{"tns_id":2297,"type": "user", "name":"TNSrobot"}'
        response = requests.get(
            url=settings.TNS_SEARCH_URL,
            headers={ 'User-Agent': tns_user_agent},
            params={
                "page": page,
                "name": "",
                "name_like": "0",
                "isTNS_AT": "all",
                "public": "all",
                "unclassified_at": "0",
                "classified_sne": "0",
                "ra": "",
                "decl": "",
                "radius": "",
                "coords_unit": "arcsec",
                "groupid[]": "null",
                "type[]": "null",
                "discoverer": "",
                "date_start[date]": pastTime,
                "date_end[date]": "2100-01-01",
                "discovery_mag_min": "",
                "discovery_mag_max": "",
                "redshift_min": "",
                "redshift_max": "",
                "spectra_count": "",
                "associated_groups[]": "null",
                "display[redshift]": "1",
                "display[hostname]": "1",
                "display[host_redshift]": "1",
                "display[source_group_name]": "1",
                "display[programs_name]": "1",
                "display[isTNS_AT]": "1",
                "display[public]": "1",
                "display[spectra_count]": "1",
                "display[discoverymag]": "1",
                "display[discmagfilter]": "1",
                "display[discoverydate]": "1",
                "display[discoverer]": "1",
                "display[sources]": "1",
                "display[bibcode]": "1",
                "num_page": resultSize,
                "edit[type]": "",
                "edit[objname]": "",
                "edit[id]": "",
                "sort": "desc",
                "order": "discoverydate",
                "format": "csv"
            },
        )

#        content = response.content
        content = response.text
        status_code = response.status_code
    except requests.exceptions.RequestException:
        print('ERROR in services/TNS/poll_tns: HTTP Request to TNS failed\n')
        print(response.text)
        sys.exit(0)

    return status_code, content

def getTNSData(opts):
    if type(opts) is dict:
        options = Struct(**opts)
    else:
        options = opts

#    import yaml
#    with open(options.configFile) as yaml_file:
#        config = yaml.load(yaml_file)

    username = settings.DB_USER_WRITE
    password = settings.DB_PASS_WRITE
    hostname = settings.DB_HOST
    database = 'ztf'
#    database = settings.DB

#    username = config['databases']['local']['username']
#    password = config['databases']['local']['password']
#    database = config['databases']['local']['database']
#    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("ERROR in services/TNS/poll_tns: Cannot connect to the database\n")
        return 1

    radius = 3.0 # arcseconds from crossmatch
    if options.radius:
        radius = float(options.radius)

    inLastNumberOfDays = None
    if options.inLastNumberOfDays:
        inLastNumberOfDays = int(options.inLastNumberOfDays)

    status_code, content = pollTNS(page = int(options.pageNumber), resultSize = int(options.pageSize), inLastNumberDays = inLastNumberOfDays)

#    csvEntries = csv.DictReader(content.splitlines(), delimiter=',')
#    data = csvEntries
    data = csv.DictReader(content.splitlines(), delimiter=',')
    rowsAdded = 0
    rowsChanged = 0
    for row in data:
        if 'Name' in row:
            name = row['Name'].strip().split()
        else:
            print(row)
            sys.exit()

        if len(name) != 2:
            prefix = 'SN'
            suffix = name[0]
        else:
            prefix = name[0]
            suffix = name[1]

        if row['Discovery Date (UT)'].strip() == '0000-00-00 00:00:00':
            # Set the discovery date to January of the suffix name.
            discoveryDate = '%s-01-01 00:00:00' % suffix[0:4]
            row['Discovery Date (UT)'] = discoveryDate

#        if not 'Type' in row:  # sometimes TNS does not supply Type -- RDW
#            row['Type'] = 'null'

        row['prefix'] = prefix
        row['suffix'] = suffix
        ra, dec = coords_sex_to_dec(row['RA'], row['DEC'])
        if ra == 0 and dec == 0:
            print("in services/TNS/poll_tns: Cannot store record for %s. No coordinates provided!\n" % row['Name'].strip())
            continue

        row['ra'] = ra
        row['dec'] = dec
        htm16 = htmCircle.htmID(16, ra, dec)
        row['htm16'] = htm16
        tnsEntry = getTNSRow(conn, suffix)
        if tnsEntry:
            if tnsEntry['tns_prefix'] != prefix:
                # The entry has been updated on TNS - classified! Otherwise do nothing!
                deleteTNSRow(conn, suffix)
                insertTNS(conn, row)
                logfile.write("Object %s has been updated\n" % row['suffix'])
                rowsChanged += 1
        else:
            insertTNS(conn, row)
            logfile.write("Object %s has been added\n" % row['suffix'])
            run_tns_crossmatch.tns_name_crossmatch(\
                    conn, row['suffix'], ra, dec, radius)

            rowsAdded += 1
        #print prefix, suffix, ra, dec, htm16, row['Discovery Date (UT)']

    logfile.write("Total rows added = %d, modified = %d\n" % (rowsAdded, rowsChanged))

    conn.commit()
    conn.close()


def main():
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    getTNSData(options)


if __name__ == '__main__':
    global logfile
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    logfile = open('/mnt/cephfs/roy/services_log/' + date + '.log', 'a')
    now = datetime.now()
    logfile.write('\n-- poll_tns at %s\n' % now.strftime("%d/%m/%Y %H:%M:%S"))
    main()
