#
# REST service to handle Sherlock requests
#

from flask import Flask, request
from flask_restful import Api, Resource, reqparse, inputs
import json
import yaml
#import subprocess
#import tempfile
#from os import unlink
from sherlock import transient_classifier

app = Flask(__name__)
api = Api(app)


conf = {
        'sherlock_settings': 'sherlock.yaml'
        }

def classify(name):
    sherlock_settings = {}
    with open(conf['sherlock_settings'], "r") as f:
        sherlock_settings = yaml.safe_load(f)
    names = []
    ra = []
    dec = []
    names.append(name)
    classifier = transient_classifier(
        settings=sherlock_settings,
        ra=ra,
        dec=dec,
        name=names,
        verbose=1,
        updateNed=False
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
        ra (float): right ascension. Required.
        dec (float): declination. Required. """

    def get(self):
        return '', 501
    def post(self):
        return self.get()


api.add_resource(Object, "/object/<string:name>")
api.add_resource(Query, "/query")

if (__name__ == '__main__'):
    app.run(debug=True, port=5000)
