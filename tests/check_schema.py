import json
from subprocess import Popen, PIPE

args = ['mysql', '--user=ztf', '--password=123password', '--execute', 'use ztf;describe objects']
process = Popen(args, stdout=PIPE, stderr=PIPE)
stdout, stderr = process.communicate()
rc = process.returncode

mysql_names = []
output = stdout.decode('utf-8')
lines = output.split('\n')[1:]
for line in lines:
    tok = line.split()
    if len(tok) > 0:
        mysql_names.append(tok[0])

schema_names = []
my_objects = json.loads(open('../utility/schema/objects.json').read())
for field in my_objects['fields']:
    schema_names.append(field['name'])

assert len(mysql_names) == len(schema_names)

for i in range(len(mysql_names)):
    assert mysql_names[i] == schema_names[i]
print('mysql and object schema identical')
