SELECT * FROM candidates INTO OUTFILE '/var/lib/mysql-files/out.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

