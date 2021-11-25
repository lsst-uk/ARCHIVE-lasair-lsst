logfile='/mnt/cephfs/lasair/services_log/'$(date +'%Y%m%d')'.log'
echo 'Pull crossmatch_tns to filter node at' $(date) >> $logfile
mysql -u ztf -p ztf -p123password < /mnt/cephfs/lasair/crossmatch_tns/crossmatch_tns.sql
mysql -u ztf -p123password ztf -e 'select count(*) from crossmatch_tns' >> $logfile
