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


CMD_INLINE  = 1


def serverCron():
    """Server side crond job."""

    loops = server.cronloops
    server.cronloops += 1

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


#------------
# PedisClient
#------------

class PedisClient(object):

    def __init__(self):
        self.cobj = None
        self.querybuf = None
        self.argc = 0
        self.argv = None
        self.flag = 0
        self.reply = None


#------------
# PedisServer
#------------

class PedisServer(object):

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

    def __init__(self, host='127.0.0.1', port=6374):
        self.host = host
        self.port = port

        #: socket object
        self.sobj = self.__tcpServer()

        self.el.createTimeEvent(1000, serverCron, None)

        self.__initConfig()

    def __initConfig(self):
        """Resolve the pedis.conf file and init server config."""

        filepath = os.environ.get(
            'PEDIS_CONFIG_FILE',
            os.path.join(os.path.dirname(__file__), '..', 'pedis.conf')
        )

        with open(filepath, 'rb') as f:
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

    def __tcpServer(self):
        """Create a tcp server. """

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(32)

        return s

    @classmethod
    def accept(self, fd, clientData):
        """Accept a client connection."""

        cobj, (host, port) = fd.accept()
        server.stat_numconnections += 1
        debug('. Accepted: {}:{}'.format(host, port))
        self.createClient(cobj)

    @classmethod
    def createClient(self, cobj):
        """Create client."""

        c = PedisClient()
        c.cobj = cobj
        c.reply = LinkList()
        self.el.createFileEvent(cobj,
                                event.READABLE,
                                self.readQueryFromClient, c)
        self.clients.addNodeTail(c)

    @classmethod
    def sendReplyToClient(self, cobj, client):
        """Send reply to client."""

        while client.reply.length:
            node = client.reply.head
            cobj.sendall(node.val)
            client.reply.delNode(node)

        if client.reply.length == 0:
            self.el.deleteFileEvent(cobj, event.WRITABLE)

    @classmethod
    def freeClient(self, cobj):
        """Free client."""

        self.el.deleteFileEvent(cobj, event.READABLE)
        self.el.deleteFileEvent(cobj, event.WRITABLE)
        cobj.close()
        server.stat_numconnections -= 1

    @classmethod
    def processCommand(self, c):
        """Process command end by clients.

        :param c: client
        """

        if c.argv[0] == 'quit':
            self.freeClient(c.cobj)
            return

        found, cmd = commands.lookup(c.argv[0])

        if not found:
            self.addReply(c, '-ERR unknown command\r\n')
            return

        cmd.proc(c)

    @classmethod
    def readQueryFromClient(self, cobj, c):
        """Read query content from client."""

        data = cobj.recv(1024)

        if len(data) == 0:
            self.freeClient(cobj)
            debug('. Client closed connection')
            return

        query = data.split()
        c.argc = len(query)
        c.argv = query
        self.processCommand(c)

    @classmethod
    def addReply(self, client, obj):
        """Add reply to the eventloop."""

        self.el.createFileEvent(client.cobj,
                                event.WRITABLE,
                                self.sendReplyToClient, client)
        client.reply.addNodeTail(obj)

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

cmd = namedtuple('cmd', ['proc', 'arity', 'flags'])


class Commands(object):

    def __init__(self):

        self.table = {
            'ping': cmd(self.ping, 1, CMD_INLINE),
            'echo': cmd(self.echo, 2, CMD_INLINE),
        }

    def lookup(self, cmd):
        """Look up given cmd.

        Returns:
            (found, cmd)
        """

        return (True, self.table[cmd]) if cmd in self.table else (False, None)

    def ping(self, c):
        server.addReply(c, shared.pong)

    def echo(self, c):
        server.addReply(c, c.argv[1])
        server.addReply(c, shared.crlf)


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
