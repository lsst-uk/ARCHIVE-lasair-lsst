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

def check_status():
    """check_status.
    Make a file in cephfs with alerts ingested today and alerts produced by ZTF
    """
    t = time.time()
    jdnow = (time.time()/86400 + 2440587.5)
    midnight = math.floor(jdnow - 0.5) + 0.5

    msl = mysql.connector.connect(\
                user    =settings.DB_USER_REMOTE, \
                password=settings.DB_PASS_REMOTE, \
                host    =settings.DB_HOST_REMOTE, \
                database='ztf')
    
    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT count(*) AS count FROM objects WHERE jdmax > %.1f' % midnight
    
    try:
        cursor.execute(query)
        for row in cursor:
            count = row['count']
            break
    except:
        sys.exit()
    
    import datetime

    g = datetime.datetime.utcnow()
    date = '%4d%02d%02d' % (g.year, g.month, g.day)
    url = 'https://monitor.alerts.ztf.uw.edu/api/datasources/proxy/7/api/v1/query?query='
    urltail = 'sum(kafka_log_log_value{ name="LogEndOffset" , night = "%s", program = "MSIP" }) - sum(kafka_log_log_value{ name="LogStartOffset", night = "%s", program="MSIP" })' % (date, date)
    
    try:
        urlquote = url + urllib.parse.quote(urltail)
        resultjson = requests.get(urlquote, 
            auth=(settings.GRAFANA_USERNAME, settings.GRAFANA_PASSWORD))
        result = json.loads(resultjson.text)
        alertsstr = result['data']['result'][0]['value'][1]
        today_candidates_ztf = int(alertsstr)
    except:
        print('Cannot parse grafana: %s' % str(result))
        today_candidates_ztf = -1
    
    update_time = datetime.datetime.utcnow().isoformat()
    update_time = update_time.split('.')[0]
    
    f = open(settings.SYSTEM_STATUS)
    status_dict = json.loads(f.read())
    f.close()

    status_dict['today_candidates_lasair'] = count
    status_dict['today_candidates_ztf']    = today_candidates_ztf
    status_dict['update_time']             = update_time

    f = open(settings.SYSTEM_STATUS, 'w')
    f.write(json.dumps(status_dict))
    f.close()
    
if __name__ == '__main__':
      check_status()
