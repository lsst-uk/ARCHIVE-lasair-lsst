SELECT * FROM objects INTO OUTFILE '/data/mysql/mysqltmp/objects.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM sherlock_classifications INTO OUTFILE '/data/mysql/mysqltmp/sherlock_classifications.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM watchlist_hits INTO OUTFILE '/data/mysql/mysqltmp/watchlist_hits.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM area_hits INTO OUTFILE '/data/mysql/mysqltmp/area_hits.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
