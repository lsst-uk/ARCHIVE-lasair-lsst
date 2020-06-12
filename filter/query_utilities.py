""" how to make a query from selects, tables, and conditions. 
Plus pages. Plus time constraints.
This is utilised for the user queries on the local database
"""

def make_query(selected, tables, conditions):
# select some quantitites from some tables
    sqlquery_real  = 'SELECT /*+ MAX_EXECUTION_TIME(300000) */ ' 
    sqlquery_real += selected

# if they added a days_ago clause, compute it here and prepent to conditions
    toktables = []
    wl_id = -1
    for table in tables.split(','):
        table = table.strip()
        if table.startswith('watchlist'):
            tok = table.split(':')
            toktables.append('watchlist_hits')
            wl_id = int(tok[1])
        else:
            toktables.append(table)

    wl_conditions = []
    if wl_id >= 0:
        wl_conditions = ['watchlist_hits.wl_id=%d' % wl_id]

    new_conditions = wl_conditions
    if len(conditions.strip()) > 0:
        new_conditions = ' AND '.join(wl_conditions + [conditions])

# list of joining conditions is prepended
    join_list = []
    if 'objects' in toktables:
        for table in toktables:
            if table != 'objects':
                join_list.append('objects.objectId = %s.objectId' % table)

    if len(new_conditions.strip()) > 0:
        join_new_conditions = ' AND '.join(join_list + [new_conditions])
    else:
        join_new_conditions = ' AND '.join(join_list)

    sqlquery_real += ' FROM ' + ','.join(toktables)
# conditions may have no where clause just order by
    if join_new_conditions.strip().lower().startswith('order'):
        sqlquery_real += ' ' + join_new_conditions
    else:
# where clause and may also have order by included
        if len(join_new_conditions.strip()) > 0:
            sqlquery_real += ' WHERE ' + join_new_conditions

    sqlquery_real += ' LIMIT 1000'
    return sqlquery_real

def topic_name(userid, name):
    name =  ''.join(e for e in name if e.isalnum() or e=='_' or e=='-' or e=='.')
    return '%d'%userid + name

