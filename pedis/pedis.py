# -*- coding: utf-8 -*-

"""
pedis.pedis
~~~~~~~~~~~

The actual server and all commands here.


- What's is INLINE cmd and BULK cmd?

    This is Redis protocal. Check out Redis docs.
"""

import random
from fnmatch import fnmatch
from server import server
from utils import shared
from _compat import pickle


CMD_INLINE  = 1
CMD_BULK = 2


@server.command(1, CMD_INLINE)
def ping(c):
    """Test connection, return PONG.

    ::
        PING
    """
    server.addReply(c, shared.pong)


@server.command(2, CMD_BULK)
def echo(c):
    """Return what you send.

    ::
        ECHO val
    """
    server.addReply(c, '${}\r\n'.format(len(c.argv[1])))
    server.addReply(c, c.argv[1])
    server.addReply(c, shared.crlf)


def _setGeneric(c, nx):
    """For set and setnx."""
    key, val = c.argv[1:]

    if nx and (key in c.dict_):
        server.addReply(c, shared.one)
        return
    c.dict_[key] = val
    if nx:
        server.addReply(c, shared.one)
    else:
        server.addReply(c, shared.ok)


@server.command(3, CMD_BULK, cmd_name='set')
def set_(c):
    """Set a key to a string value.

    ::
        SET key val
    """
    _setGeneric(c, 0)


@server.command(3, CMD_BULK)
def setnx(c):
    """Set a key to a string value if the key does not exist.

    ::
        SETNX key val
    """
    _setGeneric(c, 1)


@server.command(2, CMD_INLINE)
def get(c):
    """Return the string value of the key.

    ::
        GET key
    """
    key = c.argv[1]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
    else:
        val = c.dict_[key]
        server.addReply(c, '${}\r\n'.format(len(val)))
        server.addReply(c, repr(val))
        server.addReply(c, shared.crlf)


@server.command(2, CMD_INLINE)
def exists(c):
    """Test if a key exists.

    Replys:
        1: exists
        0: not exists

    ::
        EXISTS key
    """
    key = c.argv[1]

    if key in c.dict_:
        server.addReply(c, shared.one)
    else:
        server.addReply(c, shared.zero)


@server.command(2, CMD_INLINE)
def keys(c):
    """Return all the keys matching a given pattern.

    ::
        KEYS pattern
    """
    pattern = c.argv[1]
    keys = c.dict_.keys()
    rv = []

    for key in keys:
        if fnmatch(key, pattern):
            rv.append(key)
    if rv:
        rv = ' '.join(rv) + '\r\n'
    else:
        rv = shared.zero
    server.addReply(c, rv)


@server.command(2, CMD_INLINE, cmd_name='del')
def del_(c):
    """Delete a key.

    Replys:
        1: del ok
        0: key not exists

    ::
        DEL key
    """
    key = c.argv[1]

    try:
        del(c.dict_[key])
        server.addReply(c, shared.one)
    except KeyError:
        server.addReply(c, shared.zero)


def _incrDecr(c, x):
    """Increment or decrement key by x.

    Returns:
        0: key is not integer.
        val: current val of the key.
    """
    key = c.argv[1]

    try:
        val = c.dict_[key]
        try:
            val = int(val)
            val += x
            rv = val
            c.dict_[key] = str(val)
        except ValueError:
            rv = 0
    except KeyError:
        rv = 0
    server.addReply(c, repr(rv) + '\r\n')


@server.command(2, CMD_INLINE)
def incr(c):
    """Increment the integer value of key.

    ::
        INCR key
    """
    _incrDecr(c, 1)


@server.command(2, CMD_INLINE)
def decr(c):
    """Decrement the integer value of key.

    ::
        DECR key
    """
    _incrDecr(c, -1)


@server.command(3, CMD_INLINE)
def incrby(c):
    """Increment the integer value of key by integer.

    ::
        INCRBY key integer
    """
    try:
        x = int(c.argv[2])
    except ValueError:
        x = 0
    _incrDecr(c, x)


@server.command(3, CMD_INLINE)
def decrby(c):
    """Decrement the integer value of key by integer.

    ::
        DECRBY key integer
    """
    try:
        x = int(c.argv[2])
    except ValueError:
        x = 0
    _incrDecr(c, -x)


@server.command(2, CMD_INLINE)
def select(c):
    """Select the DB having the specified index.

    ::
        SELECT index
    """
    has_err = 0

    try:
        id_ = int(c.argv[1])
    except ValueError:
        has_err = 1

    if id_ not in range(server.dbnum):
        has_err = 1

    if has_err:
        server.addReply(c, '-ERR invalid DB index\r\n')
    else:
        debug('. Select DB: {}'.format(id_))
        c.dict_ = server.dicts[id_]
        c.dictid = id_
        server.addReply(c, shared.ok)


@server.command(1, CMD_INLINE)
def dbsize(c):
    """Return the number of keys in the current db.

    ::
        DBSIZE
    """
    server.addReply(c, repr(size) + '\r\n')


def _renameGeneric(c, nx):
    oldname, newname = c.argv[1:]

    if nx and newname in c.dict_:
        server.addReply(c, shared.one)
        return
    try:
        val = c.dict_.pop(oldname)
        c.dict_[newname] = val
        rv = shared.ok
    except KeyError:
        rv = shared.zero

    server.addReply(c, rv)


def rename(c):
    """Rename the old key in the new one, destroing the newname key
        if it already exists.

    ::
        RENAME oldname newname
    """
    self._renameGeneric(c, 0)


def renamenx(c):
    """Rename the old key in the new one, if the newname key does
        not already exists.

    ::
        RENAMENX oldname newname
    """
    self._renameGeneric(c, 1)


#------------------------------ List operations ------------------------------

def _pushGeneric(c, where):
    key, item = c.argv[1:]

    if key not in c.dict_:
        _l = []
    else:
        _l = c.dict_[key]
        if not isinstance(_l, list):
            server.addReply(c, shared.wrongtypeerr)
            return

    if where == LIST_HEAD:
        _l.insert(0, item)
    else:
        _l.append(item)

    c.dict_[key] = _l
    server.addReply(c, shared.ok)


@server.command(3, CMD_BULK)
def rpush(c):
    """Append an element to the tail of the List value at key.

    ::
        RPUSH key val
    """
    self._pushGeneric(c, LIST_TAIL)


@server.command(3, CMD_BULK)
def lpush(c):
    """Append an element to the head of the List value at key.

    ::
        LPUSH key val
    """
    self._pushGeneric(c, LIST_HEAD)


@server.command(2, CMD_INLINE)
def llen(c):
    """Return the length of the List value at key.

    ::
        LLEN key
    """
    key = c.argv[1]

    if key not in c.dict_:
        rv = shared.nil
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        rv = shared.minus2
    else:
        rv = repr(len(_l)) + '\r\n'

    server.addReply(c, rv)


@server.command(4, CMD_INLINE)
def lrange(c):
    """Return a range of elements from the List at key.

    ::
        LRANGE key start end
    """
    key, start, end = c.argv[1:]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        server.addReply(c, shared.wrongtypeerr)
        return

    try:
        start = int(start)
        end = int(end)
    except ValueError:
        server.addReply(c, shared.nil)
        return

    _range = _l[start:end]
    server.addReply(c, repr(_range) + '\r\n')


@server.command(4, CMD_BULK)
def ltrim(c):
    """Trim the list at key to the specified range of elements.

    ::
        LTRIM key start end
    """
    key, start, end = c.argv[1:]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        server.addReply(c, shared.wrongtypeerr)
        return

    try:
        start = int(start)
        end = int(end)
    except ValueError:
        server.addReply(c, shared.nil)
        return

    del l[start:end]
    server.addReply(s, shared.ok)


@server.command(3, CMD_INLINE)
def lindex(c):
    """Return the element at index position from the List at key.

    ::
        LINDEX key index
    """
    key, index, = c.argv[1:]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        server.addReply(c, shared.wrongtypeerr)
        return

    try:
        index = int(index)
    except ValueError:
        server.addReply(c, shared.nil)
        return

    try:
        item = _l[index]
        server.addReply(c, repr(item) + '\r\n')
    except IndexError:
        server.addReply(c, shared.nil)


@server.command(4, CMD_BULK)
def lset(c):
    """Set a new value as the element at index position of the
        List at key.

    ::
        LSET key index val
    """
    key, index, val = c.argv[1:]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        server.addReply(c, shared.wrongtypeerr)
        return

    try:
        index = int(index)
    except ValueError:
        server.addReply(c, shared.nil)
        return

    try:
        _l[index] = val
        server.addReply(c, shared.ok)
    except IndexError:
        server.addReply(c, "-ERR index out of range\r\n")


@server.command(4, CMD_BULK)
def lrem(c):
    """Remove the first-N, last-N, or all the elements matching
        value from the List at key.

    ::
        LREM key count value
    """
    key, count, value = c.argv[1:]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
        return

    _l = c.dict_[key]
    if not isinstance(_l, list):
        server.addReply(c, shared.wrongtypeerr)
        return

    try:
        toremove = int(count)
    except ValueError:
        server.addReply(c, shared.minus1)

    removed = 0
    if toremove >= 0:
        for x in _l:
            if toremove and _l[x] == value:
                _l.remove(x)
                toremove -= 1
                removed += 1
    else:
        for i, x in enumerate(reversed(_l)):
            if toremove and _l[x] == value:
                _i = -i - 1
                _l.pop(_i)
                toremove += 1
                removed += 1

    server.addReply(c, repr(removed) + '\r\n')


def _popGeneric(c, where):
    key = c.argv[1]

    if key not in c.dict_:
        server.addReply(c, shared.nil)
    else:
        _l = c.dict_[key]
        if not isinstance(_l, list):
            server.addReply(c, shared.wrongtypeerr)
            return
    try:
        if where == LIST_HEAD:
            item = _l.pop(0)
        else:
            item = _l.pop()
        server.addReply(c, repr(item) + '\r\n')
    except IndexError:
        server.addReply(c, shared.nil)


@server.command(2, CMD_INLINE)
def lpop(c):
    """Return and remove (atomically) the first element of the
        List at key.

    ::
        LPOP key
    """
    self._popGeneric(c, LIST_HEAD)


@server.command(2, CMD_INLINE)
def rpop(c):
    """Return and remove (atomically) the last element of the
        List at key.

    ::
        RPOP key
    """
    self._popGeneric(c, LIST_TAIL)


#------------------------------ Set operations -------------------------------

@server.command(3, CMD_BULK)
def sadd(c):
    """Add the specified member to the Set value at key.

    ::
        SADD key member
    """
    key, val = c.argv[1], c.argv[2]

    if key not in c.dict_:
        _s = set()
    else:
        _s = c.dict_[key]
        if not isinstance(_s, set):
            server.addReply(c, shared.minus2)
            return

    _s.add(val)
    c.dict_[key] = _s
    server.addReply(c, shared.one)


@server.command(3, CMD_BULK)
def srem(c):
    """Remove the specified member from the Set value at key.

    ::
        SREM key member
    """
    key, val = c.argv[1], c.argv[2]

    if key not in c.dict_:
        server.addReply(c, shared.zero)
    else:
        _s = c.dict_[key]
        if not isinstance(_s, set):
            server.addReply(c, shared.minus2)
            return

    try:
        _s.remove(val)
        rv = shared.one
    except KeyError:
        rv = shared.zero

    server.addReply(c, rv)


def scard(c):
    """Return the number of elements (the cardinality) of the
        Set at key.

    ::
        SCARD key
    """
    key = c.argv[1]

    if key not in c.dict_:
        server.addReply(c, shared.zero)
    else:
        _s = c.dict_[key]
        if not isinstance(_s, set):
            server.addReply(c, shared.minus2)
            return

    server.addReply(c, repr(len(_s)) + '\r\n')


@server.command(3, CMD_BULK)
def sismember(c):
    """Test if the specified value is a member of the Set at key.

    ::
        SISMEMBER key member
    """
    key, member = c.argv[1], c.argv[2]

    if key not in c.dict_:
        server.addReply(c, shared.zero)
    else:
        _s = c.dict_[key]
        if not isinstance(_s, set):
            server.addReply(c, shared.minus2)
            return

    if member in _s:
        rv = shared.one
    else:
        rv = shared.zero

    server.addReply(c, rv)


def _sinterGeneric(c, keys, dstkey):
    """Return the intersection between the Sets stored at key1,
        key2, ..., keyN. If dst key is not none, set the result
        to this key.
    """
    inter = None

    for key in keys:
        if key not in c.dict_:
            server.addReply(c, 'Key {!r} not exists\r\n'.format(key))
            return

        _s = c.dict_[key]

        if not isinstance(_s, set):
            server.addReply(c, shared.wrongtypeerr)
            return

        if inter is None:
            inter = _s
        else:
            inter &= _s

    if dstkey:
        self.set_(dstkey, inter)
        server.addReply(c, shared.ok)
    else:
        server.addReply(c, repr(inter) + '\r\n')


def sinter(c):
    """Return the intersection between the Sets stored at key1,
        key2, ..., keyN.

    ::
        SINTER key1 key2 ... keyN
    """
    self._sinterGeneric(c, s.argv[1:], None)


def sinterstore(c):
    """Compute the intersection between the Sets stored at key1,
        key2, ..., keyN, and store the resulting Set at dstkey.

    ::
        SINTERSTORE dstKey key1 key2 ... keyN
    """

    self._sinterGeneric(c, s.argv[2:], s.argv[1])


def smembers(c):
    """Return all the members of the Set value at key.

    ::
        SMEMBERS key
    """
    pass


@server.command(1, CMD_INLINE)
def randomkey(c):
    """Return a random key from the key space."""
    keys = list(c.dict_.keys())

    try:
        rk = random.choice(keys)
        server.addReply(c, repr(rk) + '\r\n')
    except IndexError:
        server.addReply(c, shared.nil)


def move(c):
    """Move the key from the currently selected DB to the DB
        having as index dbindex.

    ::
        MOVE key dbindex
    """
    pass


@server.command(1, CMD_INLINE)
def flushdb(c):
    """Remove all the keys of the currently selected DB.

    ::
        FLUSHDB
    """
    c.dict_.clear()
    server.addReply(c, shared.ok)


@server.command(1, CMD_INLINE)
def flushall(c):
    """Remove all the keys from all the databases.

    ::
        FLUSHALL
    """
    for i in range(server.dbnum):
        server.dicts[i].clear()
    server.addReply(c, shared.ok)


#------------------------- Persistence control commands ----------------------

def _saveDb(where):
    filepath = os.path.join(os.path.dirname(__file__), where)

    try:
        with open(filepath, 'wb') as f:
            pickle.dump(server.dicts, f)
    except IOError as e:
        warn('# Failed saving the DB: {}'.format(e))
        return 0

    debug(". DB saved on disk.")
    return 1


@server.command(1, CMD_INLINE)
def save(c):
    """Synchronously save the DB on disk."""
    if self._saveDb(server.dbfilename):
        server.addReply(c, shared.ok)
    else:
        server.addReply(c, shared.err)


@server.command(1, CMD_INLINE)
def bgsave(c):
    """Asynchronously save the DB on disk."""
    if server.bgsaveinprogress:
        server.addReply(c, '-ERR background save already in progress\r\n')
        return

    pid = os.fork()

    if pid == 0:
        server.sobj.close()
        ok = self._saveDb(server.dbfilename)
        if ok:
            exit(0)
        else:
            exit(1)
    else:
        info('Background saving started by pid {}'.format(pid))
        server.bgsaveinprogress = 1
        server.addReply(c, shared.ok)


@server.command(1, CMD_INLINE)
def lastsave(c):
    """Return the UNIX timestamp of the last successfully
       saving of the dataset on disk.
    """
    server.addReply(c, repr(server.lastsave) + '\r\n')


@server.command(1, CMD_INLINE)
def shutdown(c):
    wain('# User requested shutdown, saving DB...')
    ok = self._saveDb(server.dbfilename)
    if ok:
        wain('# Server exit now, bye bye...')
        exit(1)
    else:
        wain('# Error trying to save the DB, can\'t exit')
        server.addReply(c, '-ERR can\'t quit, problems saving the DB\r\n')


if __name__ == '__main__':
    server.run()
