"""
manage_status.py
Manage a status file that is a set of key-value pairs in JSON. 
Different processes/cores/threads can change key value or increment values.
There is a "signal" key with the increment: when this is different from
the signal value in the file, increments start at zero
"""

import datetime
import json
import time
import os, sys
SLEEPTIME = 0.1

class manage_status():
    """ manage_status.
        Args:
            signal_key: The special key, when its value changes, increment from zero
            status_file: Name of the status file
    """
    def __init__(self, signal_key, status_file):
        self.signal_key  = signal_key
        self.status_file = status_file
        self.lock_file   = status_file + '.lock'
        if not os.path.exists(self.status_file) and not os.path.exists(self.lock_file):
            print('Status file not present!')
            f = open(self.status_file, 'w')
            f.write('{}')
            f.close()

    def lock_read(self):
        """ lock_read.
            Waits for lock, then locks and returns the status
        """
        while 1:
            try:
                os.rename(self.status_file, self.lock_file)
                break
            except:
                time.sleep(SLEEPTIME)

#        print('locked   %d' % (time.time()%1000))  ##### hack hack
        f = open(self.lock_file)
        status = json.loads(f.read())
        f.close()
#        time.sleep(1)   #### hack hack
        return status

    def write_unlock(self, status):
        """ write_status:
            Writes the status file, then unlocks
            Args:
                status: dictionary of key-value pairs
        """
        update_time = datetime.datetime.utcnow().isoformat()
        update_time = update_time.split('.')[0]
        status['update_time'] = update_time
        f = open(self.lock_file, 'w')
        f.write(json.dumps(status))
        f.close()
        os.rename(self.lock_file, self.status_file)
#        print('unlocked %d' % (time.time()%1000))  ##### hack hack

    def __repr__(self):
        """ __repr__:
            Write out the status file
        """
        try:
            f = open(self.status_file)
            status = json.loads(f.read())
            f.close()
        except:
            status = {}
        return json.dumps(status, indent=2)

    def set(self, dictionary):
        """ set.
            Puts the kv pairs from the dictionary into the status file
            Args:
                dictionary: set of key-value pairs
        """
        status = self.lock_read()

        if not self.signal_key in status:
            status[self.signal_key] = 0

        for key,value in dictionary.items():
            status[key] = value

        self.write_unlock(status)

    def add(self, dictionary, signal_value):
        """ add
            Increments the given keys with the given values
            Args:
                dictionary: set of key-value pairs
                signal_value: if same as in status file, increment, else set
        """
        status = self.lock_read()

        # if the signal is not present, increment from zero
        if not self.signal_key in status:
            status[self.signal_key] = signal_value
            for key,value in dictionary.items():
                status[key]  = value

        # if the signal is present and the same as the argument, then increment if possible
        elif status[self.signal_key] == signal_value:
            for key,value in dictionary.items():
                if key in status: status[key] += value
                else:             status[key]  = value

        # if the signal has changed, increment from zero
        else:
            status = {}
            status[self.signal_key] = signal_value
            for key,value in dictionary.items():
                status[key]  = value
        self.write_unlock(status)

if __name__ == '__main__':
    for i in range(10):
        ms = manage_status('nid', './status.json')
        ms.set({'banana':5, 'orange':6})
        ms.add({'apple':12, 'pear':7}, 6)
        ms.add({'apple':12, 'pear':7}, 6)
        time.sleep(0.7)
