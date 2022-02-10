import os, sys, time
import settings

def handle(filename):
    sql  = "LOAD DATA LOCAL INFILE '%s' " % filename
    sql += "REPLACE INTO TABLE objects FIELDS TERMINATED BY ',' "
    sql += "ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

    sqlfile = 'tmp.sql'
    f = open(sqlfile, 'w')
    f.write(sql)
    f.close()

    cmd =  "mysql --user=ztf --database=ztf --password=%s --host=%s --port=%d < tmp.sql" 
    cmd = cmd % (settings.GDB_PASSWORD, settings.GDB_HOST, settings.GDB_PORT)
    os.system(cmd)

################
csvfiles = 'csvfiles'

try:
    os.mkdir('tmp')
except:
    pass

filelist = os.listdir(csvfiles)
filelist.sort()

for csvfile in filelist:
    t = time.time()
    cmd = 'cd %s; split -l 100000 %s; mv x* ../tmp' % (csvfiles, csvfile)
    print(cmd)
    os.system(cmd)
    for tmpfile in os.listdir('tmp'):
        handle('tmp/' + tmpfile)
    os.system('rm tmp/*')
    print('%s imported in %.0f seconds' % (csvfile, (time.time() - t)))

