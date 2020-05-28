ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root123password';
FLUSH PRIVILEGES;
CREATE USER 'rooty'@'%' IDENTIFIED BY 'root123password';
GRANT ALL PRIVILEGES ON *.* TO 'rooty'@'%' WITH GRANT OPTION;

CREATE DATABASE ztf;

CREATE USER 'ztf'@'localhost' IDENTIFIED BY '123password';
CREATE USER 'ztf'@'%' IDENTIFIED BY '123password';
GRANT ALL PRIVILEGES ON ztf.* TO 'ztf'@'localhost';
GRANT FILE ON *.* TO 'ztf'@'localhost';

CREATE USER 'readonly_ztf2'@'localhost' IDENTIFIED BY 'read123password';
CREATE USER 'readonly_ztf2'@'%' IDENTIFIED BY 'read123password';
GRANT SELECT ON ztf.* TO 'readonly_ztf2'@'%';
GRANT EXECUTE ON FUNCTION ztf.jdnow TO 'readonly_ztf2'@'%'

FLUSH PRIVILEGES;

