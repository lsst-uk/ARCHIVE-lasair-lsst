""" Lasair API

This class enables programmatic access to the Lasair database and Sherlock service, 
as described at http://lasair-iris.roe.ac.uk/api/.

Args:
"""
import os, sys
import requests
import json
import hashlib
import cass_utils

class LasairError(Exception):
    def __init__(self, message):
        self.message = message

class lasair_client():
    def __init__(self):
        self.cassandra_session = None

    def lightcurves(self, objectIds):
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

        if not self.cassandra_session:
            self.cassandra_session = cass_utils.cassandra()
        result = []
        for objectId in objectIds:
            record = self.cassandra_session.lightcurve(objectId)
            result.append(record)

        else:
            raise LasairError('Unknown access method %s'%access)
        return result


