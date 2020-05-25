import os, sys
import settings
file = sys.argv[1]
print(file)

sql  = "LOAD DATA LOCAL INFILE '/home/ubuntu/scratch/%s' " % file
sql += "REPLACE INTO TABLE objects FIELDS TERMINATED BY ',' "
sql += "ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

f = open('tmp.sql', 'w')
f.write(sql)
f.close()

cmd =  "mysql --user=ztf --database=ztf --password=%s < tmp.sql" % settings.DB_PASSWORD
os.system(cmd)
