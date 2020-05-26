import json
# This code reads in a forma definition of a schema, loosely based on 
# AVRO schema files (.avsc) and converts it to any of several formats
# required by Lasair

def create_table(schema):
    # Build the CREATE TABLE statement for MySQL to create this table
    tablename = schema['name']
    lines = []
    for f in schema['fields']:
        s = '`' + f['name'] + '`'
        primtype = ''
        if 'type'    in f: 
            t = f['type']
            primtype = t
            if isinstance(t, list) and len(t) == 2 and t[1] == 'null':
                primtype = t[0]
        if   primtype == 'float':  s += ' float'
        elif primtype == 'double': s += ' double'
        elif primtype == 'int':    s += ' int'
        elif primtype == 'long':   s += ' int(11)'
        elif primtype == 'bigint': s += ' bigint(20)'
        elif primtype == 'string': s += ' varchar(16)'
        elif primtype == 'bigstring': s += ' varchar(50)'
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
    # Just a list of attribute names
    list = []
    for f in schema['fields']:
        list.append(f['name'])
    return list

def autocomplete_tags(schema):
    # Something for the javascript in the autocomplete functionality on the query builder
    tablename = schema['name']
    js = ''
    for f in schema['fields']:
        js += '"' + tablename + '.' + f['name'] + ',\n'
    return js

def html(schema):
    # HTML table of attribute and description
    # NEEDS unit, UCD, etc
    s = '<h3>Schema for "%s" table</h3>\n' % schema['name']
    s += '<table border=1>\n'
    for f in schema['fields']:
        s += '<tr><td>' + f['name'] + '</td><td>' + f['doc'] + '</td></tr>\n'
    s += '</table>'
    return s

import sys
if __name__ == '__main__':
    # read in the definition file
    filename = 'object.json'
    if len(sys.argv) > 1: 
        filename = sys.argv[1]
    schema = json.loads(open(filename).read())

    # try out the methods
#    print(json.dumps(schema, indent=2))
    print(create_table(schema))
#    print(attribute_list(schema))
#    print(autocomplete_tags(schema))
#    print(html(schema))
