""" To be run in as 'screen' session that continuously fetches batches
"""
import sys
import settings
import date_nid

from subprocess import Popen, PIPE
import time

# if there is an argument,  assume it is a nid (night id) else today
while 1:
    if len(sys.argv) > 1: nid = int(sys.argv[1])
    else:                 nid  = date_nid.nid_now()

    date = date_nid.nid_to_date(nid)
    topic  = 'ztf_' + date + '_programid1'

    # where the log files go
    fh = open('/home/ubuntu/logs/' + topic + '.log', 'a')

    # make a subprocess, catch the stdout and stderr and into the log file
    args = ['python3', 'ingest.py']
    if nid: args.append('%d'%nid)
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    rc = process.returncode

    stdout = stdout.decode('utf-8')
    fh.write(stdout)
    stderr = stderr.decode('utf-8')
    fh.write(stderr)

    # if we timed out of kafka, wait a while and ask again
    if rc == 1:  # no more to get
        fh.write("END waiting %d seconds ...\n\n" % settings.INGEST_WAIT_TIME)
        fh.close()
        time.sleep(settings.INGEST_WAIT_TIME)
    # else just go ahead immediately
    else:
        fh.write("END getting more ...\n\n")
        fh.close()
