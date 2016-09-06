# -*- coding: utf-8 -*-

"""
pedis._compat
~~~~~~~~~~~~~

Compatibility for Python2 and Python3.
"""

import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle


if sys.version_info[0] == 3:
    pass
else:
    pass
