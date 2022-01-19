import os,sys, time
from datetime import datetime
from subprocess import Popen, PIPE
import settings
sys.path.append('/home/ubuntu/lasair-lsst/utility')
import date_nid

def now():
    # current UTC as string
    return datetime.utcnow().strftime("%Y/%m/%dT%H:%M:%S")

while 1:
    nid  = date_nid.nid_now()
    date = date_nid.nid_to_date(nid)
    log = open('/mnt/cephfs/lasair/services_log/' + date + '.log', 'a')

    # compute the starting conditions
    worker_args = ['--pageSize=500', '--inLastNumberOfDays=180']
    rtxt = 'poll_tns at ' +  now()
    print(rtxt)
    log.write(rtxt + '\n')

    # start the worker in subprocess
    args = ['python3', 'poll_tns.py'] + worker_args
    process = Popen(args, stdout=PIPE, stderr=PIPE)

    # fetch lines of printed output
    while 1:
        # when the worker terminates, readline returns zero
        rbin = process.stdout.readline()
        if len(rbin) == 0: break

        # if the worher uses 'print', there will be at least the newline
        rtxt = rbin.decode('utf-8').rstrip()
        log.write(rtxt + '\n')
        print(rtxt)

        # scream to the humans if ERROR
#        if rtxt.startswith('ERROR'):
#            slack_webhook.send(rtxt)

    # close the log and get the return code
    rc = process.wait()
    rtxt = 'poll_tns exit at ' +  now()
    print(rtxt)
    log.write(rtxt + '\n')
    log.close()
    time.sleep(settings.TNS_REPEAT_TIME)
