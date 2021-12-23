import os, sys
import settings
file = sys.argv[1]
tok = file.split('__')
node = tok[0]
dbtable = tok[1]
print(dbtable, ' from ', node)

sql  = "LOAD DATA LOCAL INFILE '/home/ubuntu/scratch/%s' " % file
sql += "REPLACE INTO TABLE %s FIELDS TERMINATED BY ',' " % dbtable
sql += "ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

tmpfile = '/home/ubuntu/scratch/%s.sql' % node
f = open(tmpfile, 'w')
f.write(sql)
f.close()

cmd =  "mysql --user=ztf --database=ztf --password=%s --port=%d < %s" % (settings.DB_PASSWORD, settings.DB_PORT, tmpfile)
os.system(cmd)
