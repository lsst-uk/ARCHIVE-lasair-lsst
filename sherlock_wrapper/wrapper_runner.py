"""Run the Sherlock wrapper, monitor the output and send a slack alert on errors.
Attempt to restart if the wrapper process exits, with an exponential backoff"""

import subprocess
import sys
import re
import time
import json
import slack_webhook

delay = 60
max_delay = 21600
sys.argv.pop(0)

def send_msg(m):
    try:
        slack_webhook.send(settings['slack_url'], m)
    except Exception as e:
        print ("Error sending Slack message")
        print (repr(e))


with open("/opt/lasair/wrapper_runner.json") as file:
    settings = json.load(file)

print ("Starting Sherlock wrapper.")
sys.stdout.flush()

while True:
    proc = subprocess.Popen(sys.argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        rbin = proc.stdout.readline()
        if len(rbin) == 0: break
        line = rbin.decode('UTF-8').rstrip()
        print (line)
        sys.stdout.flush()
        if re.search("(ERROR:)|(CRITICAL:)", line):
            send_msg(line)
    proc.wait()
    time.sleep(delay)
    delay = delay * 2
    if delay > max_delay:
        delay = max_delay
    print ("Attempting to restart Sherlock wrapper.")
    send_msg("Attempting to restart Sherlock wrapper.")

