import sys, json
import unittest
sys.path.append('/home/ubuntu/lasair-lsst/services/annotations')
from stream2database import process_annotations

class TestStringMethods(unittest.TestCase):

    def test_process_annotations(self):
        f = open('sample_annotations.json')
        annotations = json.loads(f.read())
        f.close()

        f = open('sample_queries.json')
        queries_from_file = json.loads(f.read())
        f.close()

        queries_from_code = process_annotations(annotations)
        for i in range(len(queries_from_file)):
            self.assertEqual(queries_from_code[i], queries_from_file[i])

if __name__ == '__main__':
    unittest.main()
