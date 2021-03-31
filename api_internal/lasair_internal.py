""" Lasair API

This class enables programmatic access to the Lasair database and Sherlock service, 
as described at http://lasair-iris.roe.ac.uk/api/.

Args:
    Each method may have this argument:
    access (string): This can change the way calls are executed. The default value 
    'file' means that lightcurves are pulled from the Ceph file system.
    The access method can also be 'cassandra'.
"""
import os, sys
import requests
import json
import hashlib
import cass_utils
BLOB_STORE_ROOT = '/mnt/cephfs/roy/objectjson'

class LasairError(Exception):
    def __init__(self, message):
        self.message = message

def dir_objectId(objectId):
    h = hashlib.md5(objectId.encode())
    return h.hexdigest()[:3]

class lasair_client():
    def __init__(self):
        self.cephfs_root = BLOB_STORE_ROOT
        self.cassandra_session = None

    def set_cephfs_root(self, cephfs_root):
        self.cephfs_root = cephfs_root

    def lightcurves(self, objectIds, access='file'):
        """ Get simple lightcurves in machine-readable form
        args:
            objectIds: list of objectIds, maximum 10
        return:
            list of dictionaries, one for each objectId. Each of these
            is a list of dictionaries, each having attributes
            candid, fid, magpsf, sigmapsf, isdiffpos, mjd
        """
        if len(objectIds) > 10:
            raise LasairError('Can only fetch 10 lightcurves for each call')

        if access == 'file':
            result = []
            for objectId in objectIds:
                fn = self.cephfs_root +'/'
                fn += dir_objectId(objectId) +'/'+ objectId + '.json'
                record = json.loads(open(fn).read())
                result.append(record)

        elif access == 'cassandra':
            if not self.cassandra_session:
                self.cassandra_session = cass_utils.cassandra()
            result = []
            for objectId in objectIds:
                record = self.cassandra_session.lightcurve(objectId)
                result.append(record)

        else:
            raise LasairError('Unknown access method %s'%access)
        return result


