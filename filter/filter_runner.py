""" To be run in as 'screen' session that continuously fetches batches
"""
import os, sys, time
import settings
from datetime import datetime
from subprocess import Popen, PIPE
sys.path.append('../utility/')
import slack_webhook

def now():
    # current UTC as string
    return datetime.utcnow().strftime("%Y/%m/%dT%H:%M:%S")

# if there is an argument, use it on the filter instances
while 1:
    arg = None
    if len(sys.argv) > 1: arg = sys.argv[1]

    # where the log files go
    if arg:
        log = open('/home/ubuntu/logs/' + arg + '.log', 'a')
    else:
        log = open('/home/ubuntu/logs/ingest.log', 'a')

    if os.path.isfile(settings.LOCKFILE):
        args = ['python3', 'filter.py']
        if arg: args.append(arg)
        print('------', now())
        process = Popen(args, stdout=PIPE, stderr=PIPE)

        while 1:
            # when the worker terminates, readline returns zero
            rbin = process.stdout.readline()
            if len(rbin) == 0: break

            # if the worher uses 'print', there will be at least the newline
            rtxt = rbin.decode('utf-8').rstrip()
            log.write(rtxt + '\n')
            print(rtxt)

            # scream to the humans if ERROR
            if rtxt.startswith('ERROR'):
                slack_webhook.send(settings.SLACK_URL, rtxt)

        while 1:
            # same with stderr
            rbin = process.stderr.readline()
            if len(rbin) == 0: break

            # if the worher uses 'print', there will be at least the newline
            rtxt = 'stderr:' + rbin.decode('utf-8').rstrip()
            log.write(rtxt + '\n')
            print(rtxt)


        process.wait()
        rc = process.returncode

        # if we timed out of kafka, wait a while and ask again
        log.write(now() + '\n')
        if rc > 0:  # no more to get
            log.write("END waiting %d seconds ...\n\n" % settings.WAIT_TIME)
            time.sleep(settings.WAIT_TIME)
        # else just go ahead immediately
        else:
            log.write("END getting more ...\n\n")
        log.close()
    else:
        # wait until the lockfile reappears
        rtxt = 'Waiting for lockfile ' + now()
        print(rtxt)
        log.write(rtxt + '\n')
        time.sleep(settings.WAIT_TIME)

