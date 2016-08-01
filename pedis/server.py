# -*- coding: utf-8 -*-

"""
pedis.server
~~~~~~~~~~~~

This is an event-driven tcp server.
"""

import event
import socket


def serverCron():
    """Server side crond job."""
    loops = server.cronloops
    server.cronloops += 1

    if loops % 3 == 0:
        print('{} clients connected.'.format(server.numconnection))
    return 1000


class PedisServer(object):

    def __init__(self, host='127.0.0.1', port=6374):
        self.host = host
        self.port = port
        self.fd = self.__tcpServer()
        self.clients = []
        self.el = event.EventLoop()
        self.cronloops = 0
        self.numconnection = 0
        self.el.createTimeEvent(1000, serverCron, None)

    def __initConfig(self):
        pass

    def __tcpServer(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(32)
        return s

    @classmethod
    def accept(self, fd):
        cobj, (host, port) = fd.accept()
        server.numconnection += 1
        print('Acceptd: {}:{}'.format(host, port))

    def run(self):
        self.el.createFileEvent(self.fd,
                                event.READABLE,
                                self.accept, None)
        self.el.main()

#: Global PedisServer object.
server = PedisServer()

if __name__ == '__main__':
    server.run()
