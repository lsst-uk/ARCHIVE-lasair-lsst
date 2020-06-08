$ sudo bash
$ mysql << EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root123password';
FLUSH PRIVILEGES;
CREATE DATABASE ztf;
USE ztf;
CREATE USER 'ztf'@'localhost' IDENTIFIED BY '123password';
CREATE USER 'ztf'@'%' IDENTIFIED BY '123password';
GRANT ALL PRIVILEGES ON ztf.* TO 'ztf'@'localhost';
GRANT FILE ON *.* TO 'ztf'@'localhost';
CREATE USER 'readonly_ztf2'@'localhost' IDENTIFIED BY 'read123password';
CREATE USER 'readonly_ztf2'@'%' IDENTIFIED BY 'read123password';
GRANT SELECT ON ztf.* TO 'readonly_ztf2'@'%';
CREATE FUNCTION jdnow () RETURNS DOUBLE DETERMINISTIC RETURN (unix_timestamp(now())/86400 + 2440587.5);
GRANT EXECUTE ON FUNCTION ztf.jdnow TO 'readonly_ztf2'@'%';
FLUSH PRIVILEGES;
EOF

cd /home/ubuntu/lasair-lsst/utility/schema
mysql -u ztf -p ztf < objects.sql
mysql -u ztf -p ztf < sherlock_crossmatches.sql
mysql -u ztf -p ztf < watchlist_hits.sql

