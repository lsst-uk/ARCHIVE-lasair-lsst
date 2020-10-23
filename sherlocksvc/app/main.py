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
from sherlock import transient_classifier

app = Flask(__name__)
api = Api(app)


conf = {
        'sherlock_settings': 'sherlock.yaml'
        }

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

class Object(Resource):
    """Get the Sherlock crossmatch results for a named object.

    Parameters:
        full (boolean): produce full output, i.e. all cross matches. Set to false to return only the top cross match. Default is true."""

    def get(self, name):
        parser = reqparse.RequestParser()
        parser.add_argument("full", type=inputs.boolean, default=True)
        message = request.data
        args = parser.parse_args()
        classifications, crossmatches = classify(name)

        if args['full']:
            return "{}, {}, {}".format(name, classifications, crossmatches), 200
        else:
            return "Hello, {} (Lite)".format(name), 200

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
        parser.add_argument("name", type=inputs.regex("^[\w\-]+$"), default="query")
        parser.add_argument("ra", type=inputs.regex("^[0-9\.]*$"), required=True)
        parser.add_argument("dec", type=inputs.regex("^[0-9\.]*$"), required=True)
        parser.add_argument("lite", type=inputs.boolean, default=False)
        message = request.data
        args = parser.parse_args()
        classifications, crossmatches = classify(
                args['name'],
                args['ra'],
                args['dec'],
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
