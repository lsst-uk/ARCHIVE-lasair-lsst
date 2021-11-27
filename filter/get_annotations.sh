logfile='/mnt/cephfs/lasair/services_log/'$(date +'%Y%m%d')'.log'
echo '\n-- pull annotations to ' $(hostname)' at' $(date) >> $logfile
mysql -u ztf -p ztf -p123password < /mnt/cephfs/lasair/annotations/annotations.sql
mysql -u ztf -p123password ztf -e 'select count(*) from annotations' >> $logfile
