# -*- coding: utf-8 -*-

"""
pedis.event
~~~~~~~~~~~

A simple event-driven programming library.
"""

import time
from select import select


__all__ = [
    'eventloop',
    'READABLE', 'WRITABLE', 'EXCEPTION',
]

READABLE = 1
WRITABLE = 2
EXCEPTION = 4

FILE_EVENTS = 1
TIME_EVENTS = 2
ALL_EVENTS = (FILE_EVENTS | TIME_EVENTS)
DONT_WAIT = 4

NOMORE = -1


class FileEvent(object):

    def __init__(self, fd, mask, fileProc, clientData):
        """File event structure.

        :param fd: client fd.
        :param mask: event type.
        :param fileProc: callback to process file event.
        """
        self.fd = fd
        self.mask = mask
        self.fileProc = fileProc
        self.clientData = clientData
        self.nextEvent = None

    def __repr__(self):
        return '<FileEvent {}>'.format(self.mask)


class TimeEvent(object):

    def __init__(self, id_, miliseconds, timeProc, clientData):
        """Time event structure.

        :param fd: client fd.
        :param mask: event type.
        :param fileProc: callback to process time event.
        """
        self.id_ = id_
        self.when = time.time() + miliseconds / 1000
        self.timeProc = timeProc
        self.clientData = clientData
        self.nextEvent = None

    def __repr__(self):
        return '<TimeEvent when={}>'.format(self.when)


class EventLoop(object):

    def __init__(self):
        """Eventloop handle two kinde of events: FileEvent and TimeEvent
        which are placed in two singlely linked-list.

        :stopFlag: Set 1 to stop eventloop.
        """
        self.fileEventHead = None
        self.timeEventHead = None
        self.timeEventNextId = 0
        self.stopFlag = 0

    def stop(self):
        self.stopFlag = 1

    def createFileEvent(self, fd, mask, fileProc, clientData):
        fe = FileEvent(fd, mask, fileProc, clientData)
        fe.nextEvent = self.fileEventHead
        self.fileEventHead = fe

    def deleteFileEvent(self, fd, mask):
        fe, prev = self.fileEventHead, None
        while fe:
            if fe.fd == fd and fe.mask == mask:
                if prev is None:
                    self.fileEventHead = fe.nextEvent
                else:
                    prev.nextEvent = fe.nextEvent
            prev = fe
            fe = fe.nextEvent
        del(fe)

    def createTimeEvent(self, miliseconds, timeProc, clientData):
        id_ = self.timeEventNextId
        self.timeEventNextId += 1
        te = TimeEvent(id_, miliseconds, timeProc, clientData)
        te.nextEvent = self.timeEventHead
        self.timeEventHead = te

    def deleteTimeEvent(self, id_):
        te, prev = self.timeEventHead, None
        while te:
            if te.id_ == id_:
                if prev is None:
                    self.timeEventHead = te
                else:
                    prev.nextEvent = te.nextEvent
            prev = te
            te = te.nextEvent
        del(te)

    def _searchNearestTimer(self):
        te = self.timeEventHead
        nearest = None

        while te is not None:
            if not nearest or te.when < nearest:
                nearest = te
            te = te.nextEvent
            return nearest

    def processEvents(self, flags):

        numfd = 0
        timeout = None
        processed = 0

        fe = self.fileEventHead

        # Nothing to do
        if not (flags & TIME_EVENTS) and \
           not (flags & FILE_EVENTS):
            return

        rfds, wfds, efds = [], [], []

        if flags & FILE_EVENTS:
            while fe is not None:
                if fe.mask & READABLE:
                    rfds += [fe.fd]
                if fe.mask & WRITABLE:
                    wfds += [fe.fd]
                if fe.mask & EXCEPTION:
                    efds += [fe.fd]
                numfd += 1
                fe = fe.nextEvent

        if numfd or ((flags & TIME_EVENTS) and not (flags & DONT_WAIT)):
            if flags & TIME_EVENTS and not flags & DONT_WAIT:
                shortest = self._searchNearestTimer()
            if shortest:
                now = time.time()
                timeout = shortest.when - now
            else:
                timeout = 0

        readyrfds, readywfds, readyefds = select(rfds, wfds, efds, timeout)
        if len(readyrfds) + len(readywfds) + len(readyefds) > 0:
            fe = self.fileEventHead
            while fe is not None:
                fd = fe.fd
                if ((fe.mask & READABLE) and (fd in readyrfds)) or \
                   ((fe.mask & WRITABLE) and (fd in readywfds)) or \
                   ((fe.mask & EXCEPTION) and (fd in readyefds)):

                    mask = 0

                    if fe.mask & READABLE and fd in readyrfds:
                        mask |= READABLE
                    if fe.mask & WRITABLE and fd in readywfds:
                        mask |= WRITABLE
                    if fe.mask & EXCEPTION and fd in readyefds:
                        mask |= EXCEPTION
                    fe.fileProc(fd, fe.clientData)
                    processed += 1
                    fe = self.fileEventHead
                    if fd in rfds:
                        rfds.remove(fd)
                    if fd in readyrfds:
                        readyrfds.remove(fd)
                    if fd in wfds:
                        wfds.remove(fd)
                    if fd in readywfds:
                        readywfds.remove(fd)
                    if fd in efds:
                        efds.remove(fd)
                    if fd in readyefds:
                        readywfds.remove(fd)
                else:
                    fe = fe.nextEvent

        if flags & TIME_EVENTS:
            te = self.timeEventHead
            maxid = self.timeEventNextId - 1
            while te:
                if te.id_ > maxid:
                    te = te.nextEvent
                    continue
                now = time.time()
                if now >= te.when:
                    rv = te.timeProc()
                    if rv != NOMORE:
                        te.when += rv / 1000
                    else:
                        self.deleteTimeEvent(te.ID)
                    te = self.timeEventHead
                else:
                    te = te.nextEvent

        return processed

    def main(self):
        while not self.stopFlag:
            self.processEvents(ALL_EVENTS)

# Singleton
eventloop = EventLoop()
