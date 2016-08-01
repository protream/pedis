import socket


host = '127.0.0.1'
port = 6374


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
