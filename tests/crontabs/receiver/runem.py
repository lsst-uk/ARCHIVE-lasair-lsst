cmds = {
    'fetch crossmatch_tns':{
    'execute':
    "/home/ubuntu/lasair-lsst/filter/get_crossmatch_tns.sh",
    },

    'fetch annotations':{
    'execute':
    "/home/ubuntu/lasair-lsst/filter/get_annotations.sh",
    },
}

import os
for name, d in cmds.items():
    os.system('date')
    print('==============')
    print(name)
    ret = os.system(d['execute'])
    if ret == 0: print('test passed')
    else:        print('test failed')
