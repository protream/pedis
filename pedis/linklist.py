# -*- coding: utf-8 -*-

"""
pedis.list
~~~~~~~~~~

A doublely linked-list encapation.
"""


__all__ = ['LinkList']


class Node(object):

    def __init__(self, val=None, prev=None, next=None):
        """Doublely linked-list node.

        :param val: the value of the node.
        :param prev: the previous list node.
        :param next: the next list node.

        """
        self.val = val
        self.prev = prev
        self.next = next


class LinkList(object):

    """A doublely linked-list implementation.

    >>> l = LinkList()
    >>> l.head == None
    True
    >>> l.tail == None
    True
    >>> l.length == 0
    True
    >>> l.addNodeHead(42)
    >>> l.head.val
    42
    >>> l.tail.val
    42
    >>> l.length
    1
    >>> l.addNodeHead('foo')
    >>> l.addNodeTail('bar')

    List -> foo <-> 42 <-> bar -> None

    >>> l.head.val
    'foo'
    >>> l.tail.val
    'bar'
    >>> l.length
    3
    >>> n = l.index(0)
    >>> n.val
    'foo'
    >>> n = l.index(1)
    >>> n.val
    42
    >>> n = l.index(-1)
    >>> n.val
    'bar'
    >>> for n in l:
    ...     print(n.val)
    foo
    42
    bar
    >>> for n in reversed(l):
    ...     print(n.val)
    bar
    42
    foo
    """

    def __init__(self):
        self.head = None
        self.tail = None
        self.length = 0

    def __repr__(self):
        return '<List length={}>'.format(self.length)

    def __len__(self):
        return self.length

    def __iter__(self):
        curr = self.head
        while curr:
            yield curr
            curr = curr.next
        raise StopIteration

    def __reversed__(self):
        curr = self.tail
        while curr:
            yield curr
            curr = curr.prev
        raise StopIteration

    def addNodeHead(self, val):
        node = Node()
        node.val = val
        if self.length == 0:
            node.prev = None
            node.next = None
            self.head = node
            self.tail = node
        else:
            node.prev = None
            node.next = self.head
            self.head.prev = node
            self.head = node
        del(node)
        self.length += 1

    def addNodeTail(self, val):
        node = Node()
        node.val = val
        if self.length == 0:
            node.prev = None
            node.next = None
            self.head = node
            self.tail = node
        else:
            node.next = None
            node.prev = self.tail
            self.tail.next = node
            self.tail = node
        self.length += 1

    def delNode(self, node):
        if node.prev:
            nove.prev.next = node.next
        else:
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        self.length -= 1

    def index(self, idx):
        """Return the node at the specified index.

                 0       1       2
               +---+   +---+   +---+
        List-->|foo|<->|42 |<->|bar|->None
               +---+   +---+   +---+
                 -3      -2      -1

        """
        if idx < 0:
            idx = (-idx) - 1
            n = self.tail
            while idx and n:
                n = n.prev
                idx -= 1
        else:
            n = self.head
            while idx and n:
                n = n.next
                idx -= 1
        return n


if __name__ == '__main__':
    import doctest
    doctest.testmod()
