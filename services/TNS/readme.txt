How to remake the TNS crossmatch

First make sure all crossmatch_tns table entries are in watchlist_cones
python3 run_tns_crossmatch.py 

Then match all the cones against the object database
python3 rebuild_watchlist_hits.py 141

