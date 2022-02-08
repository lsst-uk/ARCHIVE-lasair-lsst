import os, sys, time
import settings
filename = sys.argv[1]

sql  = "LOAD DATA LOCAL INFILE '%s' " % filename
sql += "REPLACE INTO TABLE objects FIELDS TERMINATED BY ',' "
sql += "ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

tmpfile = 'runit.sql'
f = open(tmpfile, 'w')
f.write(sql)
f.close()

cmd =  "mysql --user=ztf --database=ztf --password=%s --host=%s --port=%d < runit.sql" 
cmd = cmd % (settings.GDB_PASSWORD, settings.GDB_HOST, settings.GDB_PORT)
t = time.time()
print(cmd)
os.system(cmd)
print('%s imported in %.0f seconds' % (filename, (time.time() - t)))
