# Lasair Tests

This set of tests will eventually cover the functionality of the 
Lasair ingestion pipeline, Lasair website, and Lasair API.

# Unit and Integration Tests

### Sherlock integration
`integration_test_sherlock_wrapper.py`

This test gets some alerts from Kafka, annotates them with the Sherlock classifier,
then pushes them back into Kafka. It requires a functional Kafka and database
running on localhost and Sherlock. 
It also uese the database to check the cache capability.
It uses the following auxiliary files and directories:
* sherlock_test.yaml

### Unit tests for Sherlock wrapper
`test_sherlock_wrapper.py`

Multiple tests grouped into categories: consumer, producer, classifier.
It uses the following auxiliary files and directories:

  * example_ingested.json
  * sherlock_cache.sql

### Building INSERT query
`query_test.py`

This test checks the conversion of an alert packet to INSERT queries, both object and sherlock.
It runs the [insert_query.py](https://github.com/lsst-uk/lasair-lsst/blob/master/filter/insert_query.py) code and checks the results agains three stored alerts.
It uses the following auxiliary files and directories:
* sample_alerts *

### Building watchlist files
`watchlist_test.py`

This test uses a small watchlist to build a MOC file. Several mock alerts are run against 
the MOC file, and the number of hits asserted. The watchlist and the mock alerts are
in the same csv file, which also includes a program that generates such a csv file.
It uses the following auxiliary files and directories:
* watchlist_sample.csv
* watchlist_cache

# Deployment Tests
These tests require a deployed Lasair pipeline or webserver to run correctly.

### Schema Check
`check_schema.py`

To check that the database schemas are identical to that specified in the json files of the [master schema](https://github.com/lsst-uk/lasair-lsst/tree/master/utility/schema). For both 
objects and sherlock_classification, gets the schema from the database with `describe` and compares
the names and order of the attributes. This is important because records are transferred between
local and master database as CSV files, and if the two databases do not have identical schema, 
including the ordering of attributes, then it will not work.

### Check TNS copied to cones
`check_TNS_cones.py`

To check that every entry of the table crossmatch_tns has a corresponding entry in the table watchlist_cones.
This means that real-time crossmatch is happening between incoming alerts and the TNS objects.

### Lasair API
`api/runemall.py`

This is a battery of 3 access methods (curl, get, python),each with 7 tests (cone, lightcurves, query, sherlockobjects, sherlockposition, streamsregex, streamstopic). It requires a `settings.py` file that has the API token to run successfully.
