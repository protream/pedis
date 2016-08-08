# -*- coding: utf-8 -*-

"""
pedis.server
~~~~~~~~~~~~

::

    +------>[FileEvent]          +------------->[FileEvent]
    |            |               |                   |
    main -> acceptHandler -> createClient -> readQueryFromClient ------+
                                                                       |
                                                                       |
    addReply <- [specific]Command <- lookupCommand <- processCommand <-+
    |                 |
    +----------addReplySds
    |
    |
    +-> sendReplyToClient
    |           |
    +----->[FileEvent]

::

"""

import os.path
import socket
import random
import logging
import event
from collections import namedtuple
from linklist import LinkList


DEFAULT_DBNUM = 16

CMD_INLINE  = 1
CMD_BULK = 2

LIST_HEAD = 0
LIST_TAIL = 1


def serverCron():
    """Server side crond job."""

    loops = server.cronloops
    server.cronloops += 1

    for i in range(server.dbnum):
        dbsize = len(server.dicts[i])
        if loops % 5 == 0 and dbsize > 0:
            debug('. DB {}: {} keys in dict'.format(i, dbsize))

    if loops % 5 == 0:
        debug('. {} clients connected.'.format(server.stat_numconnections))

    return 1000


#--------------
# SharedObjects
#--------------

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


#------------
# PedisClient
#------------

class PedisClient(object):

    def __init__(self):
        self.cobj = None
        self.dict_ = None
        self.dictid = None
        self.querybuf = None
        self.argc = 0
        self.argv = None
        self.flag = 0
        self.reply = None

    def __repr__(self):
        return '<PedisClient cobj={}>'.format(self.cobj)


#------------
# PedisServer
#------------

class PedisServer(object):

    #: Python dict use as key:value database
    dicts = [{} for i in range(DEFAULT_DBNUM)]

    dbnum = DEFAULT_DBNUM

    el = event.eventloop

    #: All connected clients ared placed in this list
    clients = LinkList()

    stat_numconnections = 0

    #: Times of serverCron executed.
    cronloops = 0

    #: All supported cmds placed here
    commands = []

    logfile = None

    verbosity = None

    dbfilename = "dump.pdb"

    def __init__(self, host='127.0.0.1', port=6374):
        self.host = host
        self.port = port

        #: socket object
        self.sobj = self._tcpServer()

        self.el.createTimeEvent(1000, serverCron, None)

        self._initConfig()

    def __repr__(self):
        return '<PedisServer host={} port={}>'.format(self.host, self.port)

    def _initConfig(self):
        """Resolve the pedis.conf file and init server config."""

        filepath = os.environ.get(
            'PEDIS_CONFIG_FILE',
            os.path.join(os.path.dirname(__file__), '..', 'pedis.conf')
        )

        try:
            f = open(filepath, 'rb')
        except IOError:
            return

        for line in f.readlines():
            if line.startswith('#') or line.startswith('\n'):
                continue

            key, val = line.strip().split(' ')

            if key == 'port':
                self.port = int(val)

            elif key == 'loglevel':
                self.verbosity = {
                    'debug': logging.DEBUG,
                    'info': logging.INFO,
                    'waining': logging.WARNING,
                    'critical': logging.CRITICAL
                }.get(val, logging.DEBUG)

            elif key == 'logfile':
                if val != 'stdout':
                    self.logfile = val

            elif key == 'dir':
                pass

        f.close()

    def _tcpServer(self):
        """Create a tcp server. """

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(32)

        return s

    @classmethod
    def accept(self, sobj, clientData):
        """Accept a client connection.

        :param sobj: server socket object.
        """

        cobj, (host, port) = sobj.accept()
        server.stat_numconnections += 1
        debug('. Accepted: {}:{}'.format(host, port))
        self.createClient(cobj)

    @classmethod
    def createClient(self, cobj):
        """Create client.

        :param cobj: client connect object.
        """

        client = PedisClient()
        client.cobj = cobj
        client.dict_ = server.dicts[0]
        client.dictid = 0
        client.reply = LinkList()
        self.el.createFileEvent(cobj,
                                event.READABLE,
                                self.readQueryFromClient, client)
        self.clients.addNodeTail(client)

    @classmethod
    def sendReplyToClient(self, cobj, client):
        """Send reply to client.

        :param cobj: client connect object.
        :param client: pedis client object.
        """

        while client.reply.length:
            node = client.reply.head
            cobj.sendall(node.val)
            client.reply.delNode(node)

        if client.reply.length == 0:
            self.el.deleteFileEvent(cobj, event.WRITABLE)

    @classmethod
    def freeClient(self, cobj):
        """Free client.

        :param cobj: client connect object.
        """

        self.el.deleteFileEvent(cobj, event.READABLE)
        self.el.deleteFileEvent(cobj, event.WRITABLE)
        cobj.close()
        server.stat_numconnections -= 1

    @classmethod
    def processCommand(self, client):
        """Process command end by clients.

        :param client: pedis client object.
        """

        if client.argv[0] == 'quit':
            self.freeClient(client.cobj)
            return

        found, cmd = commands.lookup(client.argv[0])

        if not found:
            self.addReply(client, '-ERR unknown command\r\n')
            return

        if client.argc != cmd.arity:
            self.addReply(client, '-ERR wrong number of arguments\r\n')
            return

        cmd.proc(client)

    @classmethod
    def readQueryFromClient(self, cobj, client):
        """Read query content from client.

        :param cobj: client connect object.
        :param client: pedis client object.
        """

        data = cobj.recv(1024)

        if len(data) == 0:
            self.freeClient(cobj)
            debug('. Client closed connection')
            return

        query = data.split()
        client.argc = len(query)
        client.argv = query
        self.processCommand(client)

    @classmethod
    def addReply(self, client, what):
        """Add reply to the eventloop.

        :param client: pedis client object.
        :param what: content to send to the client.
        """

        self.el.createFileEvent(client.cobj,
                                event.WRITABLE,
                                self.sendReplyToClient, client)
        client.reply.addNodeTail(what)

    def run(self):
        """Run server to accept connection."""

        self.el.createFileEvent(self.sobj,
                                event.READABLE,
                                self.accept, None)
        info('- The server is now ready to accept connections.')
        self.el.main()


#---------
# Commands
#---------

#: proc: command process function
#: arity: number of arguments
#: flags: command flags
cmd = namedtuple('cmd', ['proc', 'arity', 'flags'])


class Commands(object):

    def __init__(self):
        """What is a INLINE cmd or a BULK cmd flag?

        This is a trasaction protocal used by Redis to send datas. For example:

        ::
           Todo
        ::
        """

        self.table = {
            'get': cmd(self.get, 2, CMD_INLINE),
            'set': cmd(self.set_, 3, CMD_BULK),
            'setnx': cmd(self.setnx, 3, CMD_BULK),
            'ping': cmd(self.ping, 1, CMD_INLINE),
            'del': cmd(self.del_, 2, CMD_INLINE),
            'exists': cmd(self.exists, 2, CMD_INLINE),
            'incr': cmd(self.incr, 2, CMD_INLINE),
            'decr': cmd(self.decr, 2, CMD_INLINE),
            'rpush': cmd(self.rpush, 3, CMD_BULK),
            'lpush': cmd(self.lpush, 3, CMD_BULK),
            'rpop': cmd(self.rpop, 2, CMD_INLINE),
            'lpop': cmd(self.lpop, 2, CMD_INLINE),
            'llen': cmd(self.llen, 2, CMD_INLINE),
            'lindex': cmd(self.lindex, 3, CMD_INLINE),
            'lset': cmd(self.lset, 4, CMD_BULK),
            'lrange': cmd(self.lrange, 4, CMD_INLINE),
            'ltrim': cmd(self.ltrim, 4, CMD_INLINE),
            'lrem': cmd(self.lrem, 4, CMD_BULK),
            'sadd': cmd(self.sadd, 3, CMD_BULK),
            'srem': cmd(self.sadd, 3, CMD_BULK),
            'sismember': cmd(self.sismember, 3, CMD_BULK),
            'echo': cmd(self.echo, 2, CMD_INLINE),
            'incrby': cmd(self.incrby, 3, CMD_INLINE),
            'decrby': cmd(self.decrby, 3, CMD_INLINE),
            'randomkey': cmd(self.randomkey, 1, CMD_INLINE),
            'flushdb': cmd(self.flushdb, 1, CMD_INLINE),
            'flushall': cmd(self.flushall, 1, CMD_INLINE),
            'save': cmd(self.save, 1, CMD_INLINE),
            'select': cmd(self.select, 2, CMD_INLINE),
            'shutdown': cmd(self.shutdown, 1, CMD_INLINE),
        }

    def __repr__(self):
        return '<Commands {}>'.format(list[self.table.keys()])

    def __len__(self):
        return len(self.table)

    def __getitem__(self, cmd):
        return self.table[cmd]

    def lookup(self, cmd):
        """Look up given cmd.

        Returns:
            (found, cmd)
        """

        try:
            return (1, self[cmd])
        except KeyError:
            return (0, None)

    def ping(self, c):
        """Test connection, return PONG.

        ::
            PING
        """

        server.addReply(c, shared.pong)

    def echo(self, c):
        """Return what you send.

        ::
            ECHO val
        """

        server.addReply(c, c.argv[1])
        server.addReply(c, shared.crlf)

    def _setGeneric(self, c, nx):
        """For set and setnx."""

        key, val = c.argv[1:]

        if nx and (key in c.dict_):
            server.addReply(shared.one)
            return

        c.dict_[key] = val

        if nx:
            server.addReply(c, shared.one)
        else:
            server.addReply(c, shared.ok)

    def set_(self, c):
        """Set a key to a string value.

        ::
            SET key val
        """

        self._setGeneric(c, 0)

    def setnx(self, c):
        """Set a key to a string value if the key does not exist.

        ::
            SETNX key val
        """

        self._setGeneric(c, 1)

    def get(self, c):
        """Return the string value of the key.

        ::
            GET key
        """

        key = c.argv[1]

        if key not in c.dict_:
            server.addReply(c, shared.nil)
        else:
            val = c.dict_[key]
            server.addReply(c, val + '\r\n')

    def exists(self, c):
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

    def del_(self, c):
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

    def _incrDecr(self, c, x):
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

    def incr(self, c):
        """Increment the integer value of key.

        ::
            INCR key
        """

        self._incrDecr(c, 1)

    def decr(self, c):
        """Decrement the integer value of key.

        ::
            DECR key
        """

        self._incrDecr(c, -1)

    def incrby(self, c):
        """Increment the integer value of key by integer.

        ::
            INCRBY key integer
        """

        try:
            x = int(c.argv[2])
        except ValueError:
            x = 0
        self._incrDecr(c, x)

    def decrby(self, c):
        """Decrement the integer value of key by integer.

        ::
            DECRBY key integer
        """

        try:
            x = int(c.argv[2])
        except ValueError:
            x = 0
        self._incrDecr(c, -x)

    def select(self, c):
        """Select the DB having the specified index.

        ::
            SELECT index
        """

        has_err = 0

        try:
            id_ = int(c.argv[1])
        except ValueError:
            has_err = 1

        if _id not in range(server.dbnum):
            has_err = 1

        if has_err:
            server.addReply(c, '-ERR invalid DB index\r\n')
        else:
            debug('. Select DB: {}'.format(id_))
            c.dict_ = server.dicts[id_]
            c.dictid = id_
            server.addReply(c, shared.ok)

    def dbsize(self, c):
        """Return the number of keys in the current db.

        ::
            DBSIZE
        """

        size = len(c.dict_)
        server.addReply(c, repr(size))
        server.addReply(c, shared.crlf)

    def _renameGeneric(self, c, nx):

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

    def rename(self, c):
        """Rename the old key in the new one, destroing the newname key
           if it already exists.

        ::
            RENAME oldname newname
        """

        self._renameGeneric(c, 0)

    def renamenx(self, c):
        """Rename the old key in the new one, if the newname key does
           not already exists.

        ::
            RENAMENX oldname newname
        """

        self._renameGeneric(c, 1)

    #--------------------------- list operations -------------------------------

    def _pushGeneric(self, c, where):
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

    def rpush(self, c):
        """Append an element to the tail of the List value at key.

        ::
            RPUSH key val
        """

        self._pushGeneric(c, LIST_TAIL)

    def lpush(self, c):
        """Append an element to the head of the List value at key.

        ::
            LPUSH key val
        """

        self._pushGeneric(c, LIST_HEAD)

    def _valExistsAndIsList(self, c, key):
        """Check if given key in client's dict, and if the val
           is list.

        Returns:
             0: key not exists
             1: key exists but val is not list
             2: key exists and val is list
        """

        if key not in c.dict_:
            return 0

        _l = c.dict_[key]

        if not isinstance(_l, list):
            return 1

        return 2

    def llen(self, c):
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

    def lrange(self, c):
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

    def ltrim(self, c):
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

    def lindex(self, c):
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

    def lset(self, c):
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

    def lrem(self):
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

    def _popGeneric(self, c, where):

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

    def lpop(self, c):
        """Return and remove (atomically) the first element of the
           List at key.

        ::
            LPOP key
        """

        self._popGeneric(c, LIST_HEAD)

    def rpop(self, c):
        """Return and remove (atomically) the last element of the
           List at key.

        ::
            RPOP key
        """

        self._popGeneric(c, LIST_TAIL)

    #---------------------------- set operations -------------------------------

    def sadd(self, c):
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

    def srem(self, c):
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

    def scard(self, c):
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

    def sismember(self, c):
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

    def sinter(self, c):
        """Return the intersection between the Sets stored at key1,
           key2, ..., keyN.

        ::
            SINTER key1 key2 ... keyN
        """
        pass

    def sinterstore(self, c):
        """Compute the intersection between the Sets stored at key1,
           key2, ..., keyN, and store the resulting Set at dstkey.

        ::
            SINTERSTORE dstKey key1 key2 ... keyN
        """
        pass

    def smembers(self, c):
        """Return all the members of the Set value at key.

        ::
            SMEMBERS key
        """
        pass

    def randomkey(self, c):
        """Return a random key from the key space."""

        keys = list(c.dict_.keys())

        try:
            rk = random.choice(keys)
            server.addReply(c, repr(rk) + '\r\n')
        except IndexError:
            server.addReply(c, shared.nil)

    def move(self, c):
        """Move the key from the currently selected DB to the DB
           having as index dbindex.

        ::
            MOVE key dbindex
        """

    def flushdb(self, c):
        """Remove all the keys of the currently selected DB.

        ::
            FLUSHDB
        """

        c.dict_.clear()
        server.addReply(c, shared.ok)

    def flushall(self, c):
        """Remove all the keys from all the databases.

        ::
            FLUSHALL
        """

        for i in range(server.dbnum):
            server.dicts[i].clear()
        server.addReply(c, shared.ok)

    def save(self):
        pass

    def shutdown(self, c):
        wain('# User requested shutdown, saving DB...')
        if 1:
            wain('# Server exit now, bye bye...')
            exit(1)
        else:
            wain('# Error trying to save the DB, can\'t exit')
            server.addReply(c, '-ERR can\'t quit, problems saving the DB\r\n')


#---------
# globals
#---------

#: Singletons
shared = SharedObjects()
server = PedisServer()
commands = Commands()

logging.basicConfig(level=server.verbosity,
                    filename=server.logfile, format='%(message)s')
#: Flag: '.'
debug = logging.debug
#: Flag: '-'
info = logging.info
#: Flag: '#
wain = logging.warn
#: Flag: '*'
critical = logging.critical


if __name__ == '__main__':
    server.run()
