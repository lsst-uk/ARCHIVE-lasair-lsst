logfile='/mnt/cephfs/lasair/services_log/'$(date +'%Y%m%d')'.log'
echo '\n--pull crossmatch_tns to $(hostname) at' $(date) >> $logfile
mysql -u ztf -p ztf -p123password < /mnt/cephfs/lasair/crossmatch_tns/crossmatch_tns.sql
mysql -u ztf -p123password ztf -e 'select count(*) from crossmatch_tns' >> $logfile
