import json

def create_table(schema):
    tablename = schema['name']
    lines = []
    for f in schema['fields']:
        s = '`' + f['name'] + '`'
        primtype = ''
        if 'type'    in f: 
            t = f['type']
            if isinstance(t, str):
                primtype = t
            if isinstance(t, list) and len(t) == 2 and t[1] == 'null':
                primtype = t[0]
        if   primtype == 'float':  s += ' float'
        elif primtype == 'double': s += ' double'
        elif primtype == 'int':    s += ' int'
        elif primtype == 'long':   s += ' int(11)'
        elif primtype == 'bigint': s += ' bigint(20)'
        elif primtype == 'string': s += ' varchar(16)'
        else: print('ERROR unknown type ', primtype)
    
        if 'default' in f:
            default = 'NULL'
            if f['default']: default = f['default'] 
            s += ' DEFAULT ' + default

        if 'extra' in f:
            s += ' ' + f['extra']
        lines.append(s)
    #    if 'doc'     in f and f['doc']:     s += ', ' + f['doc']
    
    sql = 'CREATE TABLE ' + schema['name'] + '(\n'
    sql += ',\n'.join(lines)

    if 'indexes' in schema:
        sql += ',\n' + ',\n'.join(schema['indexes'])

    sql += '\n)'
    return sql

def attribute_list(schema):
    list = []
    for f in schema['fields']:
        list.append(f['name'])
    return list

def autocomplete_tags(schema):
    tablename = schema['name']
    js = ''
    for f in schema['fields']:
        js += '"' + tablename + '.' + f['name'] + ',\n'
    return js

def html(schema):
    s = '<h3>Schema for "%s" table</h3>\n' % schema['name']
    s += '<table border=1>\n'
    for f in schema['fields']:
        s += '<tr><td>' + f['name'] + '</td><td>' + f['doc'] + '</td></tr>\n'
    s += '</table>'
    return s

import sys
if __name__ == '__main__':
    filename = 'object.json'
    if len(sys.argv) > 1: 
        filename = sys.argv[1]
    schema = json.loads(open(filename).read())
#    print(json.dumps(schema, indent=2))
    print(create_table(schema))
#    print(attribute_list(schema))
#    print(autocomplete_tags(schema))
#    print(html(schema))
