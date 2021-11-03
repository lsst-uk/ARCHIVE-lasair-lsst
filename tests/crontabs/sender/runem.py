cmds = {
#    'poll_tns':{
#     'execute':
#    "python3 /home/ubuntu/lasair-lsst/services/TNS/poll_tns.py --pageSize=500 --inLastNumberOfDays=180",
#},

    'replicate_tns':{
    'execute':
    "/home/ubuntu/lasair-lsst/services/TNS/dumpTNS.sh",
    'shared_space':
    "/mnt/cephfs/lasair/crossmatch_tns/crossmatch_tns.sql",
    },

    'replicate annotations':{
    'execute':
    "/home/ubuntu/lasair-lsst/services/annotations/dumpannotations.sh",
    'shared_space':
    "/mnt/cephfs/lasair/annotations/annotations.sql",
    },

    'make areas cache':{
    "execute":
    "python3 /home/ubuntu/lasair-lsst/services/areas/make_area_files.py",
    "shared_space":
    "/mnt/cephfs/lasair/areas/"
    },

    'make watchlist cache':{
    "execute":
    "python3 /home/ubuntu/lasair-lsst/services/watchlists/make_watchlist_files.py",
    "shared_space":
    "/mnt/cephfs/lasair/watchlists/"
  }
}

import os
for name, d in cmds.items():
    os.system('date')
    print('==============')
    print(name)
    ret = os.system(d['execute'])
    if ret == 0: print('test passed')
    else:        print('test failed')
    os.system('ls -l %s' % d['shared_space'])
