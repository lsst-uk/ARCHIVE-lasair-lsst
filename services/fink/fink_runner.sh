logfile='/mnt/cephfs/lasair/services_log/'$(date +'%Y%m%d')'.log'
python3 /home/ubuntu/lasair-lsst/services/fink/get_fink_annotate.py >> $logfile
