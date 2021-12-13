# Filter module #
Takes a Kafka stream of annotated JSON events, ingests them into a local MySQL,
finds coincidences with watchlists, runs query/filters on them, 
then pushes the local MySQL to the global relational database, via CSV files.

* filter_log.py
Runs the filter.py regularly, in a screen for continuous ingestion

* filter.py
The master script that does the following things in order

  * refresh.py
First clean out the local database.

  * consume_alerts.py
Runs the Kafka consumer, can be multi-process. For each alert, 
it pushes the objects and sherlock_crossmaxtches to the local database

  * insert_query.py
Computes object features and builds the INSERT query

  * mag.py
Used by insert_query for apparent magnitudes.

  * check_alerts_watchlists.py
Check a batch of alerts against the cached watchlist files, and ingests the
resulting watchlist_hits int the local database

  * run_active_queries.py and query_utilities.py
Together these two fetch and runs the users active queries and produces Kafka for them

  * output_csv.sql
Builds CSV files from the local database, to be scp'ed to the master (lasair-db)
and ingested there.

* make_watchlist_files.py
This needs to run in a crontab so that any changes to the watchlists
by users will rebuild the cached files

* date_nid.py
Utility method to deal with "night ID", which is number of days since 1/1/2017.

* settings.py
This one is not in Github, as it has passwords. Local and Master database connections, 
also the Kafka setup, the watchlist setup, etc.

* setup_mysql.sh
This one is about how to set up the MySQL for the first time, it has 
the CREATE TABLE commands that are actually ove in ../utility/schema
