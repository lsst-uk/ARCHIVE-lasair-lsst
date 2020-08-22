import sys
import requests
import urllib
import urllib.parse
import json
import time
import mysql.connector
import settings
import math
import datetime

def check_status(nid, nalert):
    """check_status.
    Make a file in cephfs with alerts ingested today and alerts produced by ZTF

    Args:
        nid: night-id associated with this ingestion
        nalert: number of alerts ingested in this batch
    """

    update_time = datetime.datetime.utcnow().isoformat()
    update_time = update_time.split('.')[0]
    
    f = open(settings.SYSTEM_STATUS)
    status_dict = json.loads(f.read())
    f.close()

    if 'nid' in status_dict and status_dict['nid'] == nid:
        status_dict['today_ingest'] += nalert
    else:
        status_dict['today_ingest'] = nalert
        status_dict['nid']          = nid

    f = open(settings.SYSTEM_STATUS, 'w')
    f.write(json.dumps(status_dict))
    f.close()
    
if __name__ == '__main__':
      check_status(123, 100)
