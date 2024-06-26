import socket
import json
import sys

HEADER_LENGTH = 10


class Client:
    def __init__(self, arg):
        self.addr = (arg[1], int(arg[2]))
        self.username = arg[3]
        self.password = arg[4]
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.socket.connect(self.addr)

    def send(self, msg):
        header = f'{len(msg):<{HEADER_LENGTH}}'.encode()
        self.socket.send(header+msg)

    def recv(self):
        header = int(self.socket.recv(HEADER_LENGTH))
        return self.socket.recv(header)

    def send_dict(self, dct):
        return self.send(json.dumps(dct, indent=4).encode())

    def get_info(self):
        return {'username': self.username,
                'password': self.password}

    def main(self):
        self.connect()
        self.send_dict(self.get_info())
        response = self.recv().decode()
        if response == 'ADDED':
            print('ACCOUNT IS ADDED')
        elif response == 'USED USERNAME':
            print('USERNAME YOU ENTERED IS ALREADY TOKEN\n'
                  'PLEASE TRY ANOTHER USERNAME')
        self.socket.close()
        sys.exit()


if __name__ == '__main__':
    # client = Client(sys.argv)
    IP = 'localhost'
    PORT = '12345'
    client = Client((None, IP, PORT, sys.argv[1], sys.argv[2]))

    client.main()
