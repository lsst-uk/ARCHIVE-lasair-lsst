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

f = open('tmp.sql', 'w')
f.write(sql)
f.close()

cmd =  "mysql --user=ztf --database=ztf --password=%s < tmp.sql" % settings.DB_PASSWORD
os.system(cmd)
