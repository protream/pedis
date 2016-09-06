import socket
from sys import argv


host = '127.0.0.1'
port = 6379


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
args = ' '.join(argv[1:])
s.sendall(args)
data = s.recv(1024)
print(data.rstrip('\r\n'))
