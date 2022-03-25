-- 2022-03-25 KWS Removed code to update File_priv column. That's what the GRANT FILE command does.
--                Also you can't update this column directly in MariaDB 10.4. Additionally removed
--                superfluous %1 and replaced with %. Was this a typo??
-- 2022-03-25 KWS This file should NOT contain any passwords. These should be picked up from Vault.
--                Likewise user IDs and DB names should be variables in the Ansible script. Renamed
--                this file to a .j2 template and so it then gets created dynamically on the server
--                e.g. in /tmp/.
CREATE DATABASE IF NOT EXISTS ztf;
USE ztf;
CREATE USER IF NOT EXISTS 'ztf'@'localhost' IDENTIFIED BY '123password';
CREATE USER IF NOT EXISTS 'ztf'@'%' IDENTIFIED BY '123password';
GRANT ALL PRIVILEGES ON ztf.* TO 'ztf'@'localhost';
GRANT FILE ON *.* TO 'ztf'@'localhost';
-- UPDATE mysql.user SET File_priv = 'Y' WHERE user='ztf';
GRANT FILE ON *.* TO 'ztf'@'%';
CREATE USER IF NOT EXISTS 'readonly_ztf2'@'localhost' IDENTIFIED BY 'read123password';
CREATE USER IF NOT EXISTS 'readonly_ztf2'@'%' IDENTIFIED BY 'read123password';
GRANT SELECT ON ztf.* TO 'readonly_ztf2'@'%';
DROP FUNCTION IF EXISTS jdnow;
CREATE FUNCTION jdnow () RETURNS DOUBLE DETERMINISTIC RETURN (unix_timestamp(now())/86400 + 2440587.5);
GRANT EXECUTE ON FUNCTION ztf.jdnow TO 'readonly_ztf2'@'%';
