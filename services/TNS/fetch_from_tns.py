import os, sys
import settings
import tempfile
import zipfile
from io import StringIO
import csv

def fetch_csv(date):
    """ Fetches CSV files from TNS
        If date is present as yyymmdd, just the delta for that date
        Else id date is 'All' get the whole TNS catalog
    """
    filename = tempfile.NamedTemporaryFile().name + '.zip'
    cmd = 'curl -s -X POST -H \'user-agent: tns_marker{"tns_id":91941,"type": "bot", "name":"Lasair_bot"}\' '
    cmd += "-d 'api_key=%s' " % settings.TNS_API_KEY

    if date == 'All':
        cmd += settings.TNS_URL + 'tns_public_objects.csv.zip '
    else:
        cmd += settings.TNS_URL + 'tns_public_objects_%s.csv.zip ' % date

    cmd += ' > %s' % filename
    os.system(cmd)
    archive = zipfile.ZipFile(filename, mode='r')

    if date == 'All':
        csvfile = 'tns_public_objects.csv'
    else:
        csvfile = 'tns_public_objects_%s.csv' % date

    content = archive.read(csvfile).decode('utf-8')
    mockfile = StringIO(content)
    reader = csv.reader(mockfile)
    rows = [row for row in reader]
    return rows[1:]    # first row is now header

if __name__ == '__main__':
    from datetime import datetime, timedelta
    g = datetime.now() - timedelta(days=1)
    yesterday = str(g).split()[0].replace('-', '')
    rows = fetch_csv(yesterday)
    #rows = fetch_csv('All')
    header = rows[0]

    print(header)
#    print('----')
#    for row in rows[1:]:
#        print(row)
