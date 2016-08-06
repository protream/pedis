# -*- coding: utf-8 -*-

"""
pedis.server
~~~~~~~~~~~~

How pedis process a command?

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
    +-> sendRepalyToClient
    |           |
    +----->[FileEvent]

"""

import os.path
import list
import event
import socket
import logging


def serverCron():
    """Server side crond job."""
    loops = server.cronloops
    server.cronloops += 1

    if loops % 5 == 0:
        logging.debug('. {} clients connected.'.format(server.stat_numconnections))
    return 1000


class SharedObjects(object):
    crlf = '\r\n'
    ok = '+OK\r\n'
    err = '-ERR\r\n'
    nil = 'nil\r\n'
    pong = '+PONG\r\n'


class PedisClient(object):

    def __init__(self):
        self.cobj = None
        self.querybuf = None
        self.argc = 0
        self.argv = None
        self.flag = 0
        self.reply = None


class PedisServer(object):

    el = event.eventloop

    #: All connected clients ared placed in this list
    clients = list.List()

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
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(32)
        return s

    @classmethod
    def accept(self, fd, clientData):
        cobj, (host, port) = fd.accept()
        server.stat_numconnections += 1
        logging.debug('. Accepted: {}:{}'.format(host, port))
        self.createClient(cobj)

    @classmethod
    def createClient(self, cobj):
        c = PedisClient()
        c.cobj = cobj
        c.reply = list.List()
        self.el.createFileEvent(cobj,
                                event.READABLE,
                                self.readQueryFromClient, c)
        self.clients.addNodeTail(c)

    @classmethod
    def sendRepalyToClient(self, cobj, client):
        while client.reply.length:
            node = client.reply.head
            cobj.sendall(node.val)
            client.reply.delNode(node)
        if client.reply.length == 0:
            self.el.deleteFileEvent(cobj, event.WRITABLE)

    @classmethod
    def freeClient(self, cobj):
        self.el.deleteFileEvent(cobj, event.READABLE)
        self.el.deleteFileEvent(cobj, event.WRITABLE)
        cobj.close()
        server.stat_numconnections -= 1

    @classmethod
    def processCommand(self, c):
        if c.argv[0] == 'ping':
            pingCommand(c)

    @classmethod
    def readQueryFromClient(self, cobj, c):
        data = cobj.recv(1024)
        if len(data) == 0:
            self.freeClient(cobj)
            return
        query = data.split()
        c.argc = len(query)
        c.argv = query
        self.processCommand(c)

    @classmethod
    def addReply(self, client, obj):
        self.el.createFileEvent(client.cobj,
                                event.WRITABLE,
                                self.sendRepalyToClient, client)
        client.reply.addNodeTail(obj)

    def run(self):
        self.el.createFileEvent(self.sobj,
                                event.READABLE,
                                self.accept, None)
        logging.info('- The server is now ready to accept connections.')
        self.el.main()

#: Global PedisServer object.
server = PedisServer()
#: Global shared object.
shared = SharedObjects()

logging.basicConfig(level=server.verbosity,
                    filename=server.logfile, format='%(message)s')


def pingCommand(c):
    server.addReply(c, shared.pong)

if __name__ == '__main__':
    server.run()
