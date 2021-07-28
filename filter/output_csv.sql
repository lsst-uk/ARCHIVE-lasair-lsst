SELECT * FROM objects INTO OUTFILE '/home/ubuntu/csvfiles/objects.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM sherlock_classifications INTO OUTFILE '/home/ubuntu/csvfiles/sherlock_classifications.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM watchlist_hits INTO OUTFILE '/home/ubuntu/csvfiles/watchlist_hits.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT * FROM area_hits INTO OUTFILE '/home/ubuntu/csvfiles/area_hits.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
