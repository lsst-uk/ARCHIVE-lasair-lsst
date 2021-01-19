from collections import OrderedDict
from gkutils.commonutils import splitList

def nullValueNULL(value):
    returnValue = None

    if value and not isinstance(value, int) and value.strip() and value != 'NULL':
        returnValue = value.strip()

    return returnValue

def boolToInteger(value):
    returnValue = value
    if value == 'true':
        returnValue = 1
    if value == 'false':
        returnValue = 0
    return returnValue


# I don't think I can change bundlesize to be anything other than 1 in Cassandra
def loadGenericCassandraTable(session, table, data, bundlesize = 1, types = None):

    if len(data) == 0:
        return

    keys = list(data[0].keys())
    typesDict = OrderedDict()

    if types is not None:
        i = 0
        for k in keys:
            typesDict[k] = types[i]
            i += 1

    formatSpecifier = ','.join(['%s' for i in keys])

    chunks = int(1.0 * len(data) / bundlesize + 0.5)
    if chunks == 0:
        subList = [data]
    else:
        bins, subList = splitList(data, bins = chunks, preserveOrder = True)


    for dataChunk in subList:
        try:
            sql = "insert into %s " % table
            sql += "(%s)" % ','.join(['%s' % k for k in keys])
            sql += " values "
            sql += ',\n'.join(['('+formatSpecifier+')' for x in range(len(dataChunk))])
            sql += ';'

            values = []
            for row in dataChunk:
                # The data comes from a CSV. We need to cast the results using the types.
                for key in keys:
                    if types is not None:
                        value = nullValueNULL(boolToInteger(row[key]))
                        if value is not None:
                            value = eval(typesDict[key])(value)
                        values.append(value)
                    # The data is already in the right python type.
                    else:
                        value = row[key]
                        values.append(value)

#            print(sql, tuple(values))
            session.execute(sql, tuple(values))


        except Exception as e:
            print("Cassandra loading EXCEPTION", e)
            #print "Error %d: %s" % (e.args[0], e.args[1])

    return

