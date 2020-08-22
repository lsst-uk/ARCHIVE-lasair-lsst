""" To be run in as 'screen' session that continuously fetches batches
"""
import os, sys
import settings

from subprocess import Popen, PIPE
import time

# if there is an argument, use it on the filter instances
while 1:
    arg = None
    if len(sys.argv) > 1: arg = sys.argv[1]

    # where the log files go
    if arg:
        fh = open('/home/ubuntu/logs/' + arg + '.log', 'a')
    else:
        fh = open('/home/ubuntu/logs/ingest.log', 'a')

    # make a subprocess, catch the stdout and stderr and into the log file
    args = ['python3', 'filter.py']
    if arg: args.append(arg)
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    rc = process.returncode
    os.system('date')

    stdout = stdout.decode('utf-8')
    fh.write(stdout)
    stderr = stderr.decode('utf-8')
    fh.write(stderr)


    # if we timed out of kafka, wait a while and ask again
#    if rc == 0:  # no more to get
    fh.write("END waiting %d seconds ...\n\n" % settings.INGEST_WAIT_TIME)
    fh.close()
    time.sleep(settings.INGEST_WAIT_TIME)
    # else just go ahead immediately
#    else:
#        fh.write("END getting more ...\n\n")
#        fh.close()
