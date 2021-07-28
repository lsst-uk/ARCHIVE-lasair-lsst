CREATE DATABASE IF NOT EXISTS ztf;
USE ztf;
CREATE USER IF NOT EXISTS 'ztf'@'localhost' IDENTIFIED BY '123password';
CREATE USER IF NOT EXISTS 'ztf'@'%1' IDENTIFIED BY '123password';
GRANT ALL PRIVILEGES ON ztf.* TO 'ztf'@'localhost';
GRANT FILE ON *.* TO 'ztf'@'localhost';
UPDATE mysql.user SET File_priv = 'Y' WHERE user='ztf';
CREATE USER IF NOT EXISTS 'readonly_ztf2'@'localhost' IDENTIFIED BY 'read123password';
CREATE USER IF NOT EXISTS 'readonly_ztf2'@'%' IDENTIFIED BY 'read123password';
GRANT SELECT ON ztf.* TO 'readonly_ztf2'@'%';
DROP FUNCTION IF EXISTS jdnow;
CREATE FUNCTION jdnow () RETURNS DOUBLE DETERMINISTIC RETURN (unix_timestamp(now())/86400 + 2440587.5);
GRANT EXECUTE ON FUNCTION ztf.jdnow TO 'readonly_ztf2'@'%';
