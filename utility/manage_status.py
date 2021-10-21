"""
manage_status.py
Manage a set of status files that is a set of key-value pairs in JSON. 
Different processes/cores/threads can change key value or increment values.
There is a "file_id" to determine which file we are working with
"""

import datetime
import json
import time
import os, sys
SLEEPTIME = 0.1

class manage_status():
    """ manage_status.
        Args:
            file_id: The special key, when its value changes, increment from zero
            status_file: Name of the status file
    """
    def __init__(self, status_file_root):
        self.status_file_root  = status_file_root

    def read(self, file_id):
        status_file = '%s_%s.json' % (self.status_file_root, str(file_id))
        f = open(status_file)
        status = json.loads(f.read())
        f.close()
        return status

    def lock_read(self, file_id):
        """ lock_read.
            If status file not present, make an empty one
            Waits for lock, then locks and returns the status
            Must be quickly followed with write_unlock!
            Args:
                file_id: which file to use
        """
        status_file = '%s_%s.json' % (self.status_file_root, str(file_id))
        lock_file   = '%s_%s.lock' % (self.status_file_root, str(file_id))

        if not os.path.exists(status_file) and not os.path.exists(lock_file):
#            print('Status file not present!')
            f = open(status_file, 'w')
            f.write('{}')
            f.close()
        # rename as lock file while we modify values
        while 1:
            try:
                os.rename(status_file, lock_file)
                break
            except:
                time.sleep(SLEEPTIME)

        # return contents
        f = open(lock_file)
        status = json.loads(f.read())
        f.close()
        return status

    def write_unlock(self, status, file_id):
        """ write_status:
            Writes the status file, then unlocks
            Args:
                status: dictionary of key-value pairs
                file_id: which file to use
        """
        update_time = datetime.datetime.utcnow().isoformat()
        update_time = update_time.split('.')[0]
        status['update_time'] = update_time

        status_file = '%s_%s.json' % (self.status_file_root, str(file_id))
        lock_file   = '%s_%s.lock' % (self.status_file_root, str(file_id))

        # dump the status to the lock file
        f = open(lock_file, 'w')
        f.write(json.dumps(status))
        f.close()
        # rename lock file as atatus file
        os.rename(lock_file, status_file)

    def tostr(self, file_id):
        """ __repr__:
            Write out the status file
        """
        status_file = '%s_%s.json' % (self.status_file_root, str(file_id))
        try:
            f = open(status_file)
            status = json.loads(f.read())
            f.close()
        except:
            status = {}
        return json.dumps(status, indent=2)

    def set(self, dictionary, file_id):
        """ set.
            Puts the kv pairs from the dictionary into the status file
            Args:
                dictionary: set of key-value pairs
                file_id: which file to use
        """
        status = self.lock_read(file_id)
        for key,value in dictionary.items():
            status[key] = value
        self.write_unlock(status, file_id)

    def add(self, dictionary, file_id):
        """ add
            Increments the given keys with the given values
            Args:
                dictionary: set of key-value pairs
                file_id: if same as in status file, increment, else set
        """
        status = self.lock_read(file_id)

        for key,value in dictionary.items():
            if key in status: status[key] += value
            else:             status[key]  = value

        self.write_unlock(status, file_id)
