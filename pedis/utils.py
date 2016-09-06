# -*- coding: utf-8 -*-

"""
pedis.utils
~~~~~~~~~~~

"""


class SharedObjects(object):
    crlf = '\r\n'
    ok = '+OK\r\n'
    err = '-ERR\r\n'
    nil = 'nil\r\n'
    pong = '+PONG\r\n'
    one = '1\r\n'
    zero = '0\r\n'
    select0 = 'select 0\r\n'
    select1 = 'select 1\r\n'
    select2 = 'select 2\r\n'
    select3 = 'select 3\r\n'
    select4 = 'select 4\r\n'
    select5 = 'select 5\r\n'
    select6 = 'select 6\r\n'
    select7 = 'select 7\r\n'
    select8 = 'select 8\r\n'
    select8 = 'select 9\r\n'
    wrongtypeerr = ("-ERR Operation against a key"
                    "holding the wrong kind of val\r\n")


shared = SharedObjects()
