import os, sys, json
import unittest
python_path = '../filter/'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), python_path)))
from run_active_queries import fetch_digest, write_digest

class TestDigestFile(unittest.TestCase):
    def test_process_annotations(self):
        topic_name = 'lasair_2WhiteDwarfdips'
        digest,last_entry,last_email = fetch_digest(topic_name)
        write_digest(digest, topic_name, last_email)

if __name__ == '__main__':
    unittest.main()
