import sys
import settings
import json
import mysql.connector
from subprocess import Popen, PIPE

def get_mysql_names():
    config = {
        'user'    : settings.DB_USER_REMOTE,
        'password': settings.DB_PASS_REMOTE,
        'host'    : settings.DB_HOST_REMOTE,
        'database': 'ztf'
    }
    msl = mysql.connector.connect(**config)

    cursor = msl.cursor(buffered=True, dictionary=True)
    query = 'describe objects'
    cursor.execute(query)
    mysql_names = []
    for row in cursor:
        mysql_names.append(row['Field'])
    return mysql_names

def get_schema_names():
    schema_names = []
    my_objects = json.loads(open('../../utility/schema/objects.json').read())
    for field in my_objects['fields']:
        schema_names.append(field['name'])
    return schema_names

if __name__ == "__main__":
    mysql_names = get_mysql_names()
    schema_names = get_schema_names()

    assert len(mysql_names) == len(schema_names)

    for i in range(len(mysql_names)):
        assert mysql_names[i] == schema_names[i]

    print('mysql and object schema identical')
