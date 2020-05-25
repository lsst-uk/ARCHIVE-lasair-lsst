import settings
import mysql.connector
config = {
    'user'    : settings.DB_USER_WRITE,
    'password': settings.DB_PASS_WRITE,
    'host'    : settings.DB_HOST_LOCAL,
    'database': 'ztf'
}
msl = mysql.connector.connect(**config)

cursor = msl.cursor(buffered=True, dictionary=True)
query = 'TRUNCATE TABLE candidates'
cursor.execute(query)

