# Lasair Services
The following services run regularly to support the Lasair ingestions pipeline.

First though, lets look at the code that runs continously in a 'screen' session
to make sure each is run at the right time, and its results logged and made 
available. They are:
- Polling the TNS for new objects and classifications of objects
- Making the watchlist MOC files in preparation for running the ingestion pipeline
- Caching the area MOC files in preparation for running the ingestion pipeline

They can run in a cron that might do the TNS polling every four hours, and the watchlist
and areas just after midnight UTC, before observations begin in California.

```
1  */4 * * * python3 /home/ubuntu/lasair-lsst/services/TNS/poll_tns.py --pageSize=500 --inLastNumberOfDays=180
11 */4 * * * /home/ubuntu/lasair-lsst/services/TNS/dumpTNS.sh 
10   0 * * * python3 /home/ubuntu/lasair-lsst/services/areas/make_area_files.py
12   0 * * * python3 /home/ubuntu/lasair-lsst/services/watchlists/make_watchlist_files.py
```

There is a special watchlist called __TNS__ that should be a mirror of the `crossmatch_tns` 
table, where the ra, dec, and `tns_name` are copied. This is how the real-time stream 
can match against TNS objects. 

Each of the following services writes its log information into the shared file system
in a directory called `service_logs`, with a fresh file every day. The web server can
see these and show the logs.

## `poll_tns.py`
This code rebuilds the database table `tns_crossmatch` by reading from the TNS server.
For each new object in TNS, it calls the function `tns_name_crossmatch` 
in `run_tns_crossmatch.py`, which does two things, one is about past matches, 
and the other about future matches.
- past matches are made explicitly by HTM query, and for each a `watchlist_hit` entry in the database.
-- future matches are ensured by adding a new `watchlist_cone` to the `__TNS__` watchlist. Furthermore, that watchlist timestamp is updated, so that its MOC file will be remade (see below).

For this code, the settings.py needs the database connection 
(`DB_HOST`, `DB_USER_WRITE`, `DB_PASS_WRITE`), and also the watchlist ID (`wl_id`) of the 
special `__TNS__` watchlist, and the endpoint of the TNS service, currently 
`TNS_SEARCH_URL` =http://wis-tns.weizmann.ac.il/search.

There is other code for checking that the `crossmatch_tns` table is complete,
these are `poll_tns_periodic.sh` and `run_all_TNS.sh`

## `dumpTNS.sh`
In order that streaming queries work properly on the filter nodes, the local database there must
have the updated TNS database, the table called crossmatch_tns. Therefore we have this process
running right after the updata of TNS that runs on the database node, and does mysqldump for that
table, putting the result in the CephFS. Each filter node should also have a crontab to fetch 
that file and build it into the local database, that is the script get_crossmatch_tns.sh.

## `make_area_files.py`
This code checks for new or updated area files uploaded to the database, and
copies them into the shared filesystem. 
Its settings.py file needs the database connection,
as well as `AREA_MOCS`, the directory where the area files are stored.

## `make_watchlist_files.py`
This code builds a MOC file for each active watchlist that has been modified more recently 
than the last time these files were made.
If a new object was added to the TNS watchlist, then it will cause that MOC to be re-made.
The settings.py for this code uses the database connection (see above), as well as:
- `WATCHLIST_MOCS`  directory to put the MOC files.
- `WATCHLIST_CHUNK` number of points for each MOC, typically 50,000, since building becomes excessively slow for large numbers of cones.
- `WATCHLIST_MAX_DEPTH` the depth of each MOC, typically 13.

## `stream2database.py`
This program gets a list of all the annotators topic names from the database, 
then pulls all the annotations that have accumulated in kafka, and
for each one builds a query and puts it in the master database.
