import os
import socket
import sys
import threading
import json

HEADER_LENGTH = 10
SERVER_PATH = 'data'
PW = 'pass.word'


class Server:
    def __init__(self, arg):
        self.addr = (arg[1], int(arg[2]))
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if not os.path.exists(SERVER_PATH):
            os.makedirs(SERVER_PATH)

    def bind_n_listen(self):
        self.socket.bind(self.addr)
        self.socket.listen()

    def main(self):
        self.bind_n_listen()
        while True:
            client, addr = self.socket.accept()
            handler = Handler(client)
            thread = threading.Thread(target=handler.main)
            thread.start()
            del handler


class Handler:
    def __init__(self, socket):
        self.socket = socket
        self.path = None
        self.username = None
        self.password = None

    def recv(self):
        header = int(self.socket.recv(HEADER_LENGTH))
        return self.socket.recv(header)

    def send(self, msg):
        header = f'{len(msg):<{HEADER_LENGTH}}'.encode()
        self.socket.send(header+msg)

    def recv_dict(self):
        return json.loads(self.recv())

    def get_info(self):
        return {'username': self.username,
                'password': self.password,
                'isconnected': False}

    def makedirs(self):
        self.path = os.path.join(SERVER_PATH, self.username)
        os.makedirs(self.path)

    def save_pw(self):
        pw_path = os.path.join(self.path, PW)
        with open(pw_path, 'w') as f:
            f.write(json.dumps(self.get_info(), indent=4))
            f.close()

    def check_availablity(self):
        recv_dict = self.recv_dict()
        self.username = recv_dict.get('username')
        self.password = recv_dict.get('password')

        users = os.listdir(SERVER_PATH)
        if self.username in users:
            self.send('USED USERNAME'.encode())
            return False
        self.send('ADDED'.encode())
        self.makedirs()
        return True

    def main(self):
        if self.check_availablity() is False:
            self.socket.close()
            return
        self.save_pw()
        self.socket.close()


if __name__ == '__main__':
    # server = Server(sys.argv)
    IP = 'localhost'
    PORT = '12345'
    server = Server((None, IP, PORT))

    server.main()
