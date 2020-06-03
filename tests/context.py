"""Import at the start of tests so that imported packages get resolved properly.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sherlock_wrapper
