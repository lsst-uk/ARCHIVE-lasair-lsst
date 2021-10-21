import os, sys, json
import unittest
python_path = '../utility/'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), python_path)))
from manage_status import manage_status

class TestAnnotations(unittest.TestCase):
    def test_manage_status(self):
        # put the status files in here
        os.system('mkdir play')

        ms = manage_status('play/status')
        ms.set({'banana':5, 'orange':6}, 6)
        ms.add({'apple':12, 'pear':7},   6)
        ms.add({'apple':12, 'pear':1},   6)

        ms.add({'apple':1, 'pear':7},    7)
        ms.add({'apple':1, 'pear':7},    7)
        ms.set({'banana':5, 'orange':6}, 7)
        ms.set({'banana':4, 'orange':7}, 7)
        ms.set({'banana':3, 'orange':8}, 7)

        status = ms.read(6)
        self.assertEqual(status['banana'], 5)
        self.assertEqual(status['orange'], 6)
        self.assertEqual(status['apple'], 24)
        self.assertEqual(status['pear'],   8)

        status = ms.read(7)
        self.assertEqual(status['banana'], 3)
        self.assertEqual(status['orange'], 8)
        self.assertEqual(status['apple'],  2)
        self.assertEqual(status['pear'],  14)

        # delete the play area
        os.system('rm -r play')

if __name__ == '__main__':
    unittest.main()
