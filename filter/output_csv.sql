SELECT * FROM objects INTO OUTFILE '/var/lib/mysql-files/objects.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
SELECT * FROM sherlock_crossmatches INTO OUTFILE '/var/lib/mysql-files/sherlock_crossmatches.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

