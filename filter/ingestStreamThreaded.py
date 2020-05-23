"""Consumes stream for ingesting to database

"""

from __future__ import print_function
import argparse
import sys
import os
import time
import settings
import mysql.connector
from mag import dc_mag
import threading
import alertConsumer


# attributes from the ZTF schema that the database knows about
wanted_attributes = [
'objectId', 'jd', 'fid', 'pid', 'diffmaglim', 'pdiffimfilename', 'programpi',
'programid', 'candid', 'isdiffpos', 'tblid', 'nid', 'rcid', 'field', 'xpos',
'ypos', 'ra', 'decl', 'magpsf', 'sigmapsf', 'chipsf', 'magap', 'sigmagap', 'distnr',
'magnr', 'sigmagnr', 'chinr', 'sharpnr', 'sky', 'magdiff', 'fwhm', 'classtar', 'mindtoedge',
'magfromlim', 'seeratio', 'aimage', 'bimage', 'aimagerat', 'bimagerat', 'elong', 'nneg',
'nbad', 'rb', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'magapbig', 'sigmagapbig',
'ranr', 'decnr', 'sgmag1', 'srmag1', 'simag1', 'szmag1', 'sgscore1', 'distpsnr1', 'ndethist',
'ncovhist', 'jdstarthist', 'jdendhist', 'scorr', 'tooflag', 'objectidps1', 'objectidps2',
'sgmag2', 'srmag2', 'simag2', 'szmag2', 'sgscore2', 'distpsnr2', 'objectidps3', 'sgmag3',
'srmag3', 'simag3', 'szmag3', 'sgscore3', 'distpsnr3', 'nmtchps', 'rfid', 'jdstartref',
'jdendref', 'nframesref', 
'rbversion', 'dsnrms', 'ssnrms', 'dsdiff', 'magzpsci', 'magzpsciunc', 'magzpscirms',
'nmatches', 'clrcoeff', 'clrcounc', 'zpclrcov', 'zpmed', 'clrmed', 'clrrms', 'neargaia',
'neargaiabright', 'maggaia', 'maggaiabright', 'exptime', 'drb', 'drbversion',
'htmid16']

def insert_sql_candidate(candidate, objectId):
    """ Creates an insert sql statement for insering the canditate info
        Also works foe candidates in the prv
    """
    names = []
    values = []

    names.append('objectId')
    values.append('"' + objectId + '"')
    for name,value in candidate.items():

        # Must not use 'dec' in mysql, so use 'decl' instead
        if name == 'dec': 
            name = 'decl'
            dec = float(value)
        if name == 'ra': 
            ra = float(value)

        if name in wanted_attributes:
            names.append(name)
            if isinstance(value, str):
                values.append('"' + value + '"')
            elif name.startswith('ss') and not value:
                values.append('-999.0')
            else:
                values.append(str(value))

        if name == 'fid':       fid = int(value)
        if name == 'magpsf':    magpsf = float(value)
        if name == 'sigmapsf':  sigmapsf = float(value)
        if name == 'magnr':     magnr = float(value)
        if name == 'sigmagnr':  sigmagnr = float(value)
        if name == 'magzpsci':  magzpsci = float(value)
        if name == 'isdiffpos': isdiffpos = value

# Compute the HTM ID for later cone searches
    htmID = 0
    names.append('htmid16')
    values.append(str(htmID))

# Compute apparent magnitude
    d = dc_mag(fid, magpsf,sigmapsf, magnr,sigmagnr, magzpsci, isdiffpos)
    names.append('dc_mag')
    values.append(str(d['dc_mag']))
    names.append('dc_sigmag')
    values.append(str(d['dc_sigmag']))


# and here is the SQL
    sql = 'INSERT IGNORE INTO candidates \n(%s) \nVALUES \n(%s)' % (','.join(names), ','.join(values))
    return sql

def insert_candidate(msl, candidate, objectId):
    """ gets the SQL for insertion, then inserts the candidate
    """
# insert the candidate record
    query = insert_sql_candidate(candidate, objectId)
    try:
        cursor = msl.cursor(buffered=True)
        cursor.execute(query)
        cursor.close()
    except mysql.connector.Error as err:
        print('INGEST Database insert candidate failed: %s' % str(err))

    msl.commit()

    return

def alert_filter(alert, msl):
    """Filter to apply to each alert.
    """
    candid = 0
    data = alert
    if data:
        print(json.dumps(alert, indent=2))
        sys.exit()
        objectId = data['objectId']
        candid   = data['candid']

# light curve features
        prv_array = data['prv_candidates']
        maglist = {'g':[], 'r':[]}
        if prv_array:
            for prv in prv_array:
                if prv['candid'] and prv['magpsf']:
                    w = [prv['jd'], prv['magpsf'], prv['sigmapsf']]
                    if prv['fid'] == 1: maglist['g'].append(w)
                    else:               maglist['r'].append(w)
           # compute features from maglist #

        insert_candidate(msl, data['candidate'], objectId)
        return candid

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', type=str,
                        help='Hostname or IP of Kafka host to connect to.')
    parser.add_argument('--topic', type=str,
                        help='Name of Kafka topic to listen to.')
    parser.add_argument('--group', type=str,
                        help='Globally unique name of the consumer group. '
                        'Consumers in the same group will share messages '
                        '(i.e., only one consumer will receive a message, '
                        'as in a queue). Default is value of $HOSTNAME.')
    parser.add_argument('--maxalert', type=int,
                        help='Max alerts to be fetched per thread')
    parser.add_argument('--nthread', type=int,
                        help='Number of threads to use')

    args = parser.parse_args()

    return args


def make_database_connection():
    msl = mysql.connector.connect(
        user     = settings.DB_USER_WRITE, 
        password = settings.DB_PASS_WRITE, 
        host     = settings.DB_HOST_LOCAL, 
        database = settings.DB_DATABASE,
        )
    return msl

class Consumer(threading.Thread):
    def __init__(self, threadID, args, conf):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conf = conf
        self.args = args

    def run(self):
        # Configure database connection
        msl = make_database_connection()
    
        # Start consumer and print alert stream
        
        try:
            streamReader = alertConsumer.AlertConsumer(self.args.topic, **self.conf)
            streamReader.__enter__()
        except alertConsumer.EopError as e:
            print('INGEST Cannot start reader: %d: %s\n' % (self.threadID, e.message))
            return
    
        if self.args.maxalert:
            maxalert = self.args.maxalert
        else:
            maxalert = 50000
    
        nalert = 0
        startt = time.time()
        while nalert < maxalert:
            try:
                msg = streamReader.poll(decode=True, timeout=settings.KAFKA_TIMEOUT)
            except alertConsumer.EopError as e:
#                print('INGEST',self.threadID, e)
                continue

            if msg is None:
#                print(self.threadID, 'null message')
                break
            else:
                for record in msg:
                    # Apply filter to each alert
                    candid = alert_filter(record, msl)
                    nalert += 1
                    if nalert%1000 == 0:
                        print('thread %d nalert %d time %.1f' % ((self.threadID, nalert, time.time()-startt)))
                        msl.close()
                        msl = make_database_connection()
    
        print('INGEST %d finished with %d alerts' % (self.threadID, nalert))

        streamReader.__exit__(0,0,0)

def main():
    args = parse_args()

    # Configure consumer connection to Kafka broker
#    print('Connecting to Kafka at %s' % args.host)
#    conf = {'bootstrap.servers': '{}:9092,{}:9093,{}:9094'.format(args.host,args.host,args.host),
#            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    conf = {'bootstrap.servers': '{}:9092'.format(args.host,args.host,args.host),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    if args.group: conf['group.id'] = args.group
    else:          conf['group.id'] = 'LASAIR'

    print('Configuration = %s' % str(conf))

    if args.nthread:
        nthread = args.nthread
    else:
        nthread = 1
    print('Threads = %d' % nthread)

    # make the thread list
    thread_list = []
    for t in range(args.nthread):
        thread_list.append(Consumer(t, args, conf))
    
    # start them up
    t = time.time()
    for th in thread_list:
         th.start()
    
    # wait for them to finish
    for th in thread_list:
         th.join()

if __name__ == '__main__':
    main()
