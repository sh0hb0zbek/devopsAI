import os
import socket
import json
import pandas as pd
import base64
import time
import datetime
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

    def close(self):
        self.socket.close()
        sys.exit()

    def authorize(self):
        self.send_dict(self.get_info())
        response = self.recv_text()
        if response == '/USERNAME':
            print('USERNAME DOES NOT EXIST!')
            return False
        elif response == '/PASSWORD':
            print('PASSWORD IS WRONG\n'
                  'PLEASE CHECK PASSWORD AND TRY AGAIN!')
            return False
        elif response == '/CONNECTED':
            print('USER HAS ALREADY CONNECTED TO SERVER\n'
                  'YOU CAN HAVE ONLY ONE CONNECTION WITH SERVER ON THE SAME TIME')
            return False
        elif response == '/AUTHORIZED':
            print('CONNECTION IS AUTHORIZED!')
            return True

    def main(self):
        self.connect()
        if self.authorize() is False:
            self.close()
        while True:
            cmd = input('> ')
            self.send_text(cmd)
            cmd = cmd.split(' ')
            if cmd[0] == 'help':
                self.help()
                continue
            elif cmd[0] =='logout':
                self.logout()
                return
            elif cmd[0] == 'list':
                self.list_files()
                continue
            elif cmd[0] == 'delete':
                self.delete()
            elif cmd[0] == 'upload_server':
                self.upload_server(cmd)
                continue
            elif cmd[0] == 'download_server':
                self.download_server(cmd)
                continue
            elif cmd[0] == 'upload_client':
                self.upload_client(cmd)
                continue
            elif cmd[0] == 'download_client':
                self.download_client(cmd)
                continue
            else:
                print('WRONG COMMAND')
                continue

    # -----commands-----
    def help(self):
        print(self.recv_text())

    def logout(self):
        print('logged out')
        self.close()

    def list_files(self):
        received_list = self.recv_list()
        if received_list == ['/PATH']:
            print('ENTERED PATH IS WRONG')
            return
        else:
            for file in received_list:
                print(f' - {file}')

    def delete(self):
        response = self.recv_text()
        if response == '/PATH':
            print('WRONG PATH!')
        elif response == '/ERROR':
            print('ERROR OCCURED. CHECK AND TRY AGAIN!')
        elif response == '/DELETED':
            print('Deleted successfully!')

    # upload_server <folder/file> <filepath> <destination>
    def upload_server(self, command):
        try:
            filetype = command[1]
            if filetype not in ['folder', 'file']:
                print('WRONG COMMAND')
                self.send_text('/ERROR')
                return
            filepath = command[2]
        except:
            print('WRONG COMMAND')
            self.send_text('/ERROR')
            return
        self.send_text('/OK')
        if filetype == 'folder':
            send_time = self.send_folder(filepath)
        else:
            send_time = self.send_file(filepath)
        if send_time == -1:
            print('ERROR OCCURED')
            return
        print(f'send_time: {send_time:.3f} s')

    def download_server(self, command):
        if self.recv_text() == '/ERROR':
            print('WRONG COMMAND')
            return
        try:
            filetype = command[1]
            if filetype not in ['folder', 'file']:
                self.send_text('/ERROR')
                return
            try:
                destination = command[3]
            except:
                destination = None
        except:
            return

        if filetype == 'folder':
            recv_time = self.recv_folder(destination=destination)
        else:
            recv_time = self.recv_file(destination=destination)
        if recv_time == -1:
            print('ERROR OCCURED')
            return
        print(f'send_time: {recv_time:.3f} s')

    # upload_client <username/all>-<key> <folder/file> <filepath>
    def upload_client(self, command):
        response = self.recv_text()
        if response == 'USERNAME':
            print('USERNAME DOES NOT EXIST')
            return
        elif response == '/ERROR':
            print('ERROR OCCURED')
        try:
            filetype = command[2]
            filepath = command[3]
            self.upload_server([None, filetype, filepath])
        except:
            print('WRONG COMMAND')
            return

    # download_client <username/all>-<key> <destination>
    def download_client(self, command):
        st_time = time.time()
        response = self.recv_text()
        if response == '/WRONG_INPUT':
            print('USERNAME/KEY WRONG')
            return
        elif response == '/ERROR':
            print('ERROR OCCURED 1')
            return
        try:
            destination = command[2]
            # self.download_server([None, filetype, None, destination])
        except:
            print('WRONG COMMAND')
            return
        try:
            nfiles = self.recv_text()
            if nfiles == '/ERROR':
                print('ERROR OCCURED 2')
                return
            nfiles = int(nfiles)
            for _ in range(nfiles):
                filetype = self.recv_text()
                if filetype == 'folder':
                    self.recv_folder(destination)
                else:
                    self.recv_file(destination)
            end_time = time.time() - st_time
            print(f'download_time: {end_time:.3f} s')
        except:
            print('ERROR OCCURED 2')
            return

    # -----help functions----
    def send(self, msg):
        header = f'{len(msg):<{HEADER_LENGTH}}'.encode()
        self.socket.send(header + msg)

    def recv(self):
        header = int(self.socket.recv(HEADER_LENGTH))
        return self.socket.recv(header)

    def send_text(self, txt):
        self.send(txt.encode())

    def recv_text(self):
        return self.recv().decode()

    def send_list(self, lst):
        self.send(json.dumps(lst).encode())

    def recv_list(self):
        return json.loads(self.recv())

    def send_dict(self, dct):
        self.send(json.dumps(dct, indent=4).encode())

    def recv_dict(self):
        return json.loads(self.recv())

    def send_file(self, filepath):
        st_time = time.time()
        if os.path.exists(filepath) is False:
            print('WRONG FILE PATH')
            self.send_text('/ERROR')
            return -1
        fname = os.path.split(filepath)[-1]
        self.send_text(fname)
        if fname[-4:] == '.csv':
            filetype = 'data-sheet'
        else:
            filetype = 'file'
        self.send_text(filetype)
        if filetype == 'file':
            try:
                with open(filepath, 'rb') as f:
                    data = {'data': base64.b64encode(f.read()).decode(),
                            'timestamp': str(datetime.datetime.utcnow().isoformat(sep=' ', timespec='milliseconds'))}
                    self.send_dict(data)
                    f.close()
                    if self.recv_text() == '/ERROR':
                        print('ERROR OCCURED')
                        return -1
            except:
                self.send_text('/ERROR')
                print('ERROR OCCURED')
                return -1
        elif filetype == 'data-sheet':
            try:
                finished = False
                skiprow = 0
                nrows = 40
                while not finished:
                    df = pd.read_csv(filepath, skiprows=range(1, skiprow), nrows=nrows)
                    if df.shape[0] < nrows:
                        finished = True
                        nrows = df.shape[0]
                    result = df.to_json(orient='records')
                    parsed = json.loads(result)
                    for i in range(nrows):
                        temp = {'timestamp': datetime.datetime.utcnow().isoformat(sep=' ', timespec='milliseconds')}
                        parsed[i].update(temp)
                        if finished:
                            if i == nrows-1:
                                parsed[i].update({'finished': True})
                        self.send_dict(parsed[i])
                        if self.recv_text() == '/ERROR':
                            print('ERROR OCCURED')
                            return -1
                    skiprow += nrows
            except:
                self.send_text('/ERROR')
                print('ERROR OCCURED!')
                return
        end_time = time.time() - st_time
        return end_time

    def recv_file(self, destination=None):
        st_time = time.time()
        filename = self.recv_text()
        if filename == '/ERROR':
            print('WRONG COMMAND')
            return -1
        filetype = self.recv_text()
        if destination is None:
            filepath = filename
        else:
            filepath = os.path.join(destination, filename)
            if os.path.exists(destination) is False:
                os.makedirs(destination)
        if filetype == 'file':
            try:
                received_dict = self.recv_dict()
                with open(filepath, 'wb') as f:
                    data = received_dict['data'].encode()
                    f.write(data)
                    # f.write(received_dict['data'].encode())
                    f.close()
                    self.send_text('/OK')
            except:
                self.send_text('/ERROR')
                print('ERROR OCCURED')
                return -1
        elif filetype == 'data-sheet':
            try:
                finished = False
                data = list()
                while not finished:
                    try:
                        received_dict = self.recv_dict()
                        if received_dict.get('finished'):
                            finished = True
                            del received_dict['finished']
                        del received_dict['timestamp']
                        data.append(received_dict)
                        self.send_text('/OK')
                    except:
                        self.send_text('/ERROR')
                        print('ERROR OCCURED')
                        return -1
                data = pd.DataFrame(data)
                data.to_csv(filepath)
            except:
                self.send_text('/ERROR')
                return -1
        end_time = time.time() - st_time
        return end_time

    def send_folder(self, folder_path):
        st_time = time.time()
        if os.path.exists(folder_path) is False:
            print('WRONG COMMAND')
            self.send_text('/ERROR')
            return -1
        foldername = os.path.split(folder_path)[-1]
        self.send_text(foldername)
        files = os.listdir(folder_path)
        self.send_text(str(len(files)))
        for file in files:
            filepath = os.path.join(folder_path, file)
            self.send_file(filepath)
            if self.recv_text() == '/ERROR':
                print('ERROR OCCURED')
                return -1
        end_time = time.time() - st_time
        return end_time

    def recv_folder(self, destination=None):
        st_time = time.time()
        foldername = self.recv_text()
        if foldername == '/ERROR':
            print('ERROR OCCURED')
            return -1
        if destination is None:
            destination = foldername
        else:
            destination = os.path.join(destination, foldername)
        nfiles = self.recv_text()
        nfiles = int(nfiles)
        for _ in range(nfiles):
            if self.recv_file(destination=destination) == -1:
                self.send_text('/ERROR')
                return -1
            self.send_text('/OK')
        end_time = time.time() - st_time
        return end_time

    def get_info(self):
        return {'username': self.username,
                'password': self.password}


if __name__ == '__main__':
    # client = Client(sys.argv)
    IP = 'localhost'
    PORT = '23456'
    client = Client((None, IP, PORT, sys.argv[1], sys.argv[2]))
    client.main()
