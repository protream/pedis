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
import logging
import event
from collections import namedtuple
from linklist import LinkList


DEFAULT_DBNUM = 16

CMD_INLINE  = 1
CMD_BULK = 2


def serverCron():
    """Server side crond job."""

    loops = server.cronloops
    server.cronloops += 1

    for i in range(server.dbnum):
        numkeys = len(server.dicts[i].keys())
        if loops % 5 == 0 and numkeys > 0:
            debug('. DB {}: {} keys in dict'.format(i, numkeys))

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

    #: Python dict use as key:value databse
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
        """What is a INLINE cmd or a BULK cmd?

        This is a protocal used by Redis to send datas. For example:

        ::
           Todo
        ::
        """

        self.table = {
            'ping': cmd(self.ping, 1, CMD_INLINE),
            'echo': cmd(self.echo, 2, CMD_INLINE),
            'get': cmd(self.get, 2, CMD_INLINE),
            'set': cmd(self.set_, 3, CMD_BULK),
            'setnx': cmd(self.setnx, 3, CMD_BULK),
            'del': cmd(self.del_, 2, CMD_INLINE),
            'incr': cmd(self.incr, 2, CMD_INLINE),
            'decr': cmd(self.decr, 2, CMD_INLINE),
            'incrby': cmd(self.incrby, 3, CMD_INLINE),
            'decrby': cmd(self.decrby, 3, CMD_INLINE),
            'exists': cmd(self.exists, 2, CMD_INLINE),
            'save': cmd(self.save, 1, CMD_INLINE),
            'select': cmd(self.select, 2, CMD_INLINE),
        }

    def __repr__(self):
        return '<Commands {}>'.format(list[self.table.keys()])

    def __len__(self):
        return len(self.table)

    def lookup(self, cmd):
        """Look up given cmd.

        Returns:
            (found, cmd)
        """

        return (1, self.table[cmd]) if cmd in self.table else (0, None)

    def ping(self, c):
        """Test connection, return PONG.

        ::
            PING
        """

        server.addReply(c, shared.pong)

    def echo(self, c):
        """Echo what you send.

        ::
            ECHO val
        """

        server.addReply(c, c.argv[1])
        server.addReply(c, shared.crlf)

    def _setGeneric(self, c, nx):
        """For set and setnx."""

        key, val = c.argv[1], c.argv[2]

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
            server.addReply(c, val)
            server.addReply(c, shared.crlf)

    def exists(self, c):
        """Test if a key exists.

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
        """Incrment or decrement key by x.

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

        server.addReply(c, str(rv))
        server.addReply(c, shared.crlf)

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

        has_err = False

        try:
            id_ = int(c.argv[1])
        except ValueError:
            has_err = True

        if id_ < 0 or id_ >= server.dbnum:
            has_err = True

        if has_err:
            server.addReply(c, '-ERR invalid DB index\r\n')
        else:
            debug('. Select DB: {}'.format(id_))
            c.dict_ = server.dicts[id_]
            c.dictid = id_
            server.addReply(c, shared.ok)

    def save(self):
        pass


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
