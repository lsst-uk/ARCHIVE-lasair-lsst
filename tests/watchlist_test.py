"""Test watchlist infrstucture
"""

import unittest.main
from unittest import TestCase, expectedFailure
import json
import logging
from time import sleep
import context



class WatchlistTest(TestCase):

    @classmethod
    def setUpClass(cls):

        with open("example_ingested.json", 'r') as f:
            # load example data
            data = json.load(f)
            # try to delete the topics
            del_topic(conf['input_topic'])
            del_topic(conf['output_topic'])
            # write example messages
            p = Producer(settings)
            try:
                for alert in data:
                    p.produce(conf['input_topic'], value=json.dumps(alert))
            finally:
                p.flush(timeout=20)

    @classmethod
    def tearDownClass(cls):
        del_topic(conf['input_topic'])
        del_topic(conf['output_topic'])
        pass

    def test_1_run_wrapper(self):
        logging.basicConfig(level=logging.WARNING)
        log = logging.getLogger("sherlock_wrapper_test")
        wrapper.run(conf,log)

    def test_2_check_result(self):
        logging.basicConfig(level=logging.WARNING)
        log = logging.getLogger("sherlock_wrapper_test")

        # should be 5 alerts
        self.assertEqual(len(alerts), 5)
        # all alerts shuould have a sherlock classification of ORPHAN
        for alert in alerts:
            self.assertEqual(alert['sherlock_classification'], "ORPHAN") 


#if __name__ == '__main__':
#    import xmlrunner
#    runner = xmlrunner.XMLTestRunner(output='test-reports')
#    unittest.main(testRunner=runner)
#    unittest.main()


from filter.make_watchlist_files import rebuild_cache
from filter.check_alerts_watchlists import check_alerts_against_watchlists
from filter.check_alerts_watchlists import read_watchlist_cache_files
cache_dir = 'watchlist_cache/'
chunk_size = 50000

def test_cache():
    cone_ids = []
    cone_ralist   = []
    cone_delist   = []
    cone_radius   = []
    cone_names    = []
    alert_ralist = []
    alert_delist = []

    for line in open('watchlist_sample.csv').readlines():
        if line[0] == '#': continue
        tok = line.strip().split(',')
        if len(tok) == 5:   # cone
            cone_ids.append(     int(tok[0]))
            cone_ralist.append(float(tok[1]))
            cone_delist.append(float(tok[2]))
            cone_radius.append(float(tok[3]))
            cone_names.append(       tok[4])

    cones = {'cone_ids':cone_ids, 'ra':cone_ralist, 'de':cone_delist, 'radius':cone_radius, 'names':cone_names}
    wl_id   = 42
    wl_name = 'watchlist_sample'
    max_depth = 13
    rebuild_cache(wl_id, wl_name, cones, max_depth, cache_dir, chunk_size)

def test_alerts():
    alert_ralist = []
    alert_delist = []
    alert_objlist = []

    for line in open('watchlist_sample.csv').readlines():
        if line[0] == '#': continue
        tok = line.strip().split(',')
        if len(tok) == 3:   # cone
            alert_ralist.append(float(tok[0]))
            alert_delist.append(float(tok[1]))
            alert_objlist.append(tok[2])
    alertlist = {"obj":alert_objlist, "ra":alert_ralist, "de":alert_delist}

    watchlistlist = read_watchlist_cache_files(cache_dir)
    hits = check_alerts_against_watchlists(alertlist, watchlistlist, chunk_size)
    if hits:
        for hit in hits: print(hit)

#####
test_cache()
test_alerts()
