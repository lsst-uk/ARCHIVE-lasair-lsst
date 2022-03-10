#!/usr/bin/env python
"""
Run the TNS refresher, putting logs where Lasair can see them
"""

import os,sys, time
from datetime import datetime
from subprocess import Popen, PIPE
import settings
sys.path.append('/home/ubuntu/lasair-lsst/utility')
import date_nid

def now():
    # current UTC as string
    return datetime.utcnow().strftime("%Y/%m/%dT%H:%M:%S")

nid  = date_nid.nid_now()
date = date_nid.nid_to_date(nid)
log = open('/mnt/cephfs/lasair/services_log/' + date + '.log', 'a')

rtxt = 'poll_tns at ' +  now()
log.write(rtxt + '\n')

# start the worker in subprocess -- yesterday's update
args = ['python3', 'poll_tns.py', '--daysAgo=1']
process = Popen(args, stdout=PIPE, stderr=PIPE)

# fetch lines of printed output
while 1:
    # when the worker terminates, readline returns zero
    rbin = process.stdout.readline()
    if len(rbin) == 0: break

    # if the worher uses 'print', there will be at least the newline
    rtxt = rbin.decode('utf-8').rstrip()
    log.write(rtxt + '\n')

    # scream to the humans if ERROR
    if rtxt.startswith('ERROR'):
        slack_webhook.send(rtxt)

# close the log and get the return code
rc = process.wait()
rtxt = 'poll_tns exit at ' +  now()
log.write(rtxt + '\n')
log.close()
