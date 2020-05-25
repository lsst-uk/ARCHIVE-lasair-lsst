import os, sys
file = sys.argv[1]
print(file)

sql  = "LOAD DATA LOCAL INFILE '/home/ubuntu/scratch/%s' " % file
sql += "REPLACE INTO TABLE candidates FIELDS TERMINATED BY ',' "
sql += "ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

f = open('tmp.sql', 'w')
f.write(sql)
f.close()

cmd =  "mysql --user=ztf --database=ztf --password=123password < tmp.sql"
os.system(cmd)
