# -*- coding: utf-8 -*-
"""
pedis.test_pedis
~~~~~~~~~~~~~~~~

some tests.
"""

import socket
from nose.tools import ok_


s = None


def setUp():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def tearDown():
    s.close()


def test_add():
    print(s)
