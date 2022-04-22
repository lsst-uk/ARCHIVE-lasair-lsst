#
# REST service to handle Sherlock requests
#

from flask import Flask, request
from flask_restful import Api, Resource, reqparse, inputs
import json
import yaml
#import logging
#import subprocess
#import tempfile
#from os import unlink
import pymysql.cursors
from sherlock import transient_classifier

app = Flask(__name__)
api = Api(app)


conf = {
        'settings_file': 'sherlock_service_settings.yaml',
        'sherlock_settings': 'sherlock_settings.yaml'
        }

class NotFoundException(Exception):
    pass

# run the sherlock classifier
def classify(name,ra,dec,lite=False):
    with open(conf['sherlock_settings'], "r") as f:
        sherlock_settings = yaml.safe_load(f)
        classifier = transient_classifier(
            log=app.logger,
            settings=sherlock_settings,
            ra=ra,
            dec=dec,
            name=name,
            verbose=1,
            updateNed=False,
            lite=lite
        )
        classifications, crossmatches = classifier.classify()
        return classifications, crossmatches

# look up the dec and ra for a name
def lookup(names):
    ra = []
    dec = []
    with open(conf['settings_file'], "r") as f:
        settings = yaml.safe_load(f)
        connection = pymysql.connect(
            host=settings['database']['host'],
            user=settings['database']['username'],
            password=settings['database']['password'],
            db=settings['database']['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)        
        for name in names:
            query = "SELECT ramean,decmean FROM objects WHERE objectID='{}'".format(name)
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    ra.append(result['ramean'])
                    dec.append(result['decmean'])
                else:
                    raise NotFoundException("Object {} not found".format(name))
        connection.close()
        return ra, dec

class Object(Resource):
    """Get the Sherlock crossmatch results for a named object.

    Parameters:
        lite (boolean): produce top ranked matches only. Default False."""

    def get(self, name):
        parser = reqparse.RequestParser()
        parser.add_argument("lite", type=inputs.boolean, default=False)
        message = request.data
        args = parser.parse_args()
        names = name.split(',')
        try:
            ra, dec = lookup(names)
        except NotFoundException as e:
            return {"message":str(e)}, 404

        classifications, crossmatches = classify(names, ra, dec, args['lite'])
        result = {
            'classifications': classifications,
            'crossmatches': crossmatches
            }
        return result, 200

    def post(self, name):
        return self.get(name)

class Query(Resource):
    """Run Sherlock for a given position.

    Parameters:
        name (string): Name of object. Optional.
        ra (float): right ascension. Required.
        dec (float): declination. Required.
        lite (boolean): produce top ranked matches only. Default False."""

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("name", type=inputs.regex("^[\w\-,]+$"), default="")
        parser.add_argument("ra", type=inputs.regex("^[0-9\.,]*$"), required=True)
        parser.add_argument("dec", type=inputs.regex("^[0-9\.\-,]*$"), required=True)
        parser.add_argument("lite", type=inputs.boolean, default=False)
        message = request.data
        args = parser.parse_args()
        name = args['name'].split(',')
        ra = args['ra'].split(',')
        dec = args['dec'].split(',')
        if len(ra) != len(dec):
            return "ra and dec lists must be equal length", 400
        if len(name) != len(ra):
            name = []
            for i in range(len(ra)):
                name.append("query"+str(i))
        classifications, crossmatches = classify(
                name,
                ra,
                dec,
                lite=args['lite'])
        result = {
                'classifications': classifications,
                'crossmatches': crossmatches
                }
        return result, 200
    def post(self):
        return self.get()


api.add_resource(Object, "/object/<string:name>")
api.add_resource(Query, "/query")

if (__name__ == '__main__'):
    app.run(debug=True, port=5000)
