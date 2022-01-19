import sys
import settings
import mysql.connector
sys.path.append('/home/ubuntu/lasair-lsst/utility')
import date_nid
from manage_status import manage_status

def main():
    msl = mysql.connector.connect(
        user    =settings.READWRITE_USER,
        password=settings.READWRITE_PASS,
        host    =settings.DB_HOST,
        port    =settings.DB_PORT,
        database='ztf'
    )

    if not msl:
         print("ERROR in services/TNS/poll_tns: Cannot connect to the database\n")

    cursor = msl.cursor(buffered=True, dictionary=True)
    cursor.execute ("select count(*) as countAnnotations from annotations")
    for row in cursor:
        countAnnotations = row['countAnnotations']
    cursor.close ()

    ms = manage_status(settings.SYSTEM_STATUS)
    nid = date_nid.nid_now()
    ms.set({'countAnnotations':countAnnotations}, nid)

if __name__ == '__main__':
    main()

