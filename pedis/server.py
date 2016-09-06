# -*- coding: utf-8 -*-

"""
pedis.server
~~~~~~~~~~~~

::

    +------>[FileEvent]          +------------->[FileEvent]
    |            |               |                   |
    server -> acceptHandler -> createClient -> readQueryFromClient ----+
                                                                       |
                                                                       |
    addReply <- [specific]Command <- lookupCommand <- processCommand <-+
    |                 |
    +----------addReplySds
    |
    |
    +-> sendReplyToClient -> client
    |           |
    +----->[FileEvent]

::

"""

import os
import socket
import logging
import event
from collections import namedtuple
from multiprocessing import Process
from linklist import LinkList
from utils import shared
from _compat import pickle


DEFAULT_DBNUM = 16

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

    if server.bgsaveinprogress:
        pass

    return 1000


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


class PedisServer(object):

    dbnum = DEFAULT_DBNUM

    el = event.eventloop

    #: All connected clients ared placed in this list
    clients = LinkList()

    stat_numconnections = 0

    commands = {}

    #: Times of serverCron executed.
    cronloops = 0

    #: A Unix timestamp
    lastsave = None

    logfile = None

    verbosity = None

    dbfilename = "dump.pdb"

    bgsaveinprogress = 0

    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port

        self.dicts = self._initDb()

        #: socket object
        self.sobj = self._tcpServer()

        self.el.createTimeEvent(1000, serverCron, None)

        self._initConfig()

    def __repr__(self):
        return '<PedisServer host={} port={}>'.format(self.host, self.port)

    def _initDb(self):
        filepath = os.path.join(os.path.dirname(__file__), 'dump.pdb')
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        # Have no dump file, init empty db
        return [{} for i in range(self.dbnum)]

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

    def command(self, arity, flags, cmd_name=None):
        def decorator(f):
            name = cmd_name if cmd_name else f.__name__
            self.commands[name] = cmd(f, arity, flags)
            return f
        return decorator

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
    def lookup_command(self, cmd):
        """Look up given cmd.

        Returns:
            (found, cmd)
        """
        try:
            return (1, self.commands[cmd])
        except KeyError:
            return (0, None)

    @classmethod
    def processCommand(self, client):
        """Process command end by clients.

        :param client: pedis client object.
        """
        if client.argv[0] == 'quit':
            self.freeClient(client.cobj)
            return

        found, cmd = self.lookup_command(client.argv[0])

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


#: proc: command process function
#: arity: number of arguments
#: flags: command flags
cmd = namedtuple('cmd', ['proc', 'arity', 'flags'])
server = PedisServer()
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
