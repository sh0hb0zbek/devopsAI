import base64
import json
import socket
import sys
import threading
import pandas as pd
import os
import time
import datetime
import shutil

HEADER_LENGTH = 10
SERVER_PATH = 'data'
PW = 'pass.word'
USERS = dict()


def update_users():
    while True:
        clients = os.listdir(SERVER_PATH)
        for client in clients:
            pw_path = os.path.join(SERVER_PATH, client, PW)
            try:
                with open(pw_path, 'r') as f:
                    info = json.loads(f.read())
                    USERS.update({info['username']: info})
                    f.close()
            except:
                continue


class Server:
    def __init__(self, arg):
        self.addr = (arg[1], int(arg[2]))
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.path.exists(SERVER_PATH) is False:
            os.makedirs(SERVER_PATH)
        self.last_update = 0

    def bind_n_listen(self):
        self.socket.bind(self.addr)
        self.socket.listen()

    def main(self):
        self.bind_n_listen()

        updater = threading.Thread(target=update_users)
        updater.start()

        while True:
            client, addr = self.socket.accept()
            print(f'[NEW CONNECTION] established from {addr}')
            handler = Handler(client)
            thread = threading.Thread(target=handler.main)
            thread.start()
            del thread


class Handler:
    def __init__(self, socket):
        self.socket = socket
        self.username = None
        self.password = None
        self.isconnected = False
        self.share_path = 'share'

    def authorize(self):
        info = self.recv_dict()
        self.set_info(info)
        if self.username not in USERS.keys():
            self.send_text('/USERNAME')
            return False
        if self.password != USERS[self.username]['password']:
            self.send_text('/PASSWORD')
            return False
        if USERS[self.username]['isconnected']:
            self.send_text('/CONNECTED')
            return False
        self.send_text('/AUTHORIZED')
        self.isconnected = True
        self.save_pw()
        return True

    def close(self):
        self.socket.close()

    def main(self):
        if self.authorize() is False:
            self.close()
            return

        while True:
            cmd = self.recv_text().split(' ')
            if cmd[0] == 'help':
                self.help()
                continue
            elif cmd[0] == 'logout':
                self.logout()
                return
            elif cmd[0] == 'list':
                self.list_files(cmd)
            elif cmd[0] == 'upload_server':
                self.upload_server(cmd)
                continue
            elif cmd[0] == 'delete':
                self.delete(cmd)
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
                continue

    # -----commands-----
    def help(self):
        help_msg = 'help ----------------------------------------------------------\n' \
                   'list <directory(optional)> ------------------------------------\n' \
                   'delete <filepath/directory> -----------------------------------\n' \
                   'upload_server <folder/file> <path> <destination(optinal)> -----\n' \
                   'upload_client <username/all>-<key> <folder/file> <filepath> ---\n' \
                   'download_server <folder/file> <path> <destination(optinal)> ---\n' \
                   'download_client <username/all>-key <destination> --------------'
        self.send_text(help_msg)

    def logout(self):
        self.isconnected = False
        self.save_pw()
        self.close()

    def list_files(self, command=None):
        try:
            path = command[1]
        except IndexError:
            path = None
        if path is None:
            list_path = os.path.join(SERVER_PATH, self.username)
        else:
            list_path = os.path.join(SERVER_PATH, self.username, path)
        if os.path.exists(list_path):
            self.send_list(os.listdir(list_path))
        else:
            self.send_list(['/PATH'])

    def delete(self, command):
        try:
            path = command[1]
        except IndexError:
            self.send_text('/PATH')
            return
        file_path = os.path.join(SERVER_PATH, self.username, path)
        if os.path.exists(file_path) is False:
            self.send_text('/PATH')
            return
        if os.path.isdir(file_path):
            if os.listdir(file_path) == list():
                os.rmdir(file_path)
            else:
                shutil.rmtree(file_path, ignore_errors=True)
        elif os.path.isfile(file_path):
            os.remove(file_path)
        else:
            self.send_text('/ERROR')
            return
        self.send_text('/DELETED')

    # upload_server <folder/file> <filepath> <destination>
    def upload_server(self, command):
        if self.recv_text() == '/ERROR':
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
            self.recv_folder(destination=destination)
        else:
            self.recv_file(destination=destination)
        return

    # download_server <folder/file> <filepath> <destination>
    def download_server(self, command, client=False):
        try:
            filetype = command[1]
            if filetype not in ['folder', 'file']:
                self.send_text('/ERROR')
                return
            filepath = command[2]
        except:
            self.send_text(f'/ERROR')
            return
        self.send_text('/OK')
        if client:
            filepath = os.path.join(SERVER_PATH, self.username, self.share_path, filepath)
        else:
            filepath = os.path.join(SERVER_PATH, self.username, filepath)
        if filetype == 'folder':
            self.send_folder(filepath)
        else:
            self.send_file(filepath)
        return

    # upload_client <username/all>-key <folder/file> <filepath>
    def upload_client(self, command):
        try:
            username, key = command[1].split('-')
            if username not in os.listdir(SERVER_PATH) and username != 'all':
                self.send_text('USERNAME')
                return
            filetype = command[2]
            self.send_text('/OK')
        except:
            self.send_text('/ERROR')
            return
        filepath = os.path.join(self.share_path, f'{username}-{key}')
        try:
            self.upload_server([None, filetype, None, filepath])
        except:
            return

    # download_client <username/all>-<key> <destination>
    def download_client(self, command):
        try:
            username, key = command[1].split('-')
            filepath = os.path.join(SERVER_PATH, username, self.share_path)
            share_list = os.listdir(filepath)
            if f'{self.username}-{key}' in share_list:
                filepath = os.path.join(filepath, f'{self.username}-{key}')
            elif f'all-{key}' in share_list:
                filepath = os.path.join(filepath, f'all-{key}')
            else:
                self.send_text('/WRONG_INPUT')
                return
            self.send_text('/OK')
        except:
            self.send_text('/ERROR')
            return
        try:
            items = os.listdir(filepath)
            self.send_text(str(len(items)))
            for item in items:
                item_path = os.path.join(filepath, item)
                if os.path.isdir(item_path):
                    self.send_text('folder')
                    self.send_folder(item_path)
                else:
                    self.send_text('file')
                    self.send_file(item_path)
        except:
            self.send_text('/ERROR')
            return

    # -----help functions-----
    def send(self, msg):
        header = f'{len(msg):<{HEADER_LENGTH}}'.encode()
        self.socket.send(header + msg)

    def recv(self):
        header = int(self.socket.recv(HEADER_LENGTH))
        return self.socket.recv(header)

    def send_text(self, txt):
        self.send(txt.encode())

    def send_list(self, lst):
        self.send(json.dumps(lst).encode())

    # def recv_list(self):
    #     return json.loads(self.recv())

    def recv_text(self):
        return self.recv().decode()

    def send_dict(self, dct):
        self.send(json.dumps(dct, indent=4).encode())

    def recv_dict(self):
        return json.loads(self.recv())

    def send_file(self, filepath):
        st_time = time.time()
        if os.path.exists(filepath) is False:
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
                    data = {'data': f.read().decode(),
                            'timestamp': str(datetime.datetime.utcnow().isoformat(sep=' ', timespec='milliseconds'))}
                    self.send_dict(data)
                    f.close()
                    if self.recv_text() == '/ERROR':
                        return -1
            except:
                self.send_text('/ERROR')
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
                            if i == nrows - 1:
                                parsed[i].update({'finished': True})
                        self.send_dict(parsed[i])
                        if self.recv_text() == '/ERROR':
                            return -1
                    skiprow += nrows
            except:
                self.send_text('/ERROR')
                return
        end_time = time.time() - st_time
        return end_time

    def recv_file(self, destination=None):
        st_time = time.time()
        filename = self.recv_text()
        if filename == '/ERROR':
            return -1
        filetype = self.recv_text()
        if filetype == 'file':
            try:
                received_dict = self.recv_dict()
                if destination is None:
                    filepath = os.path.join(SERVER_PATH, self.username)
                else:
                    filepath = os.path.join(SERVER_PATH, self.username, destination)
                    if os.path.exists(filepath) is False:
                        os.makedirs(filepath)
                with open(os.path.join(filepath, filename), 'wb') as f:
                    data = base64.b64decode(received_dict['data'].encode())
                    f.write(data)
                    # f.write(received_dict['data'].encode())
                    f.close()
                    self.send_text('/OK')
            except:
                self.send_text('/ERROR')
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
                        return -1
                data = pd.DataFrame(data)
                if destination is None:
                    filepath = os.path.join(SERVER_PATH, self.username)
                else:
                    filepath = os.path.join(SERVER_PATH, self.username, destination)
                    if os.path.exists(filepath) is False:
                        os.makedirs(filepath)
                data.to_csv(os.path.join(filepath, filename))
            except:
                self.send_text('/ERROR')
                return -1
        end_time = time.time() - st_time
        return end_time

    def send_folder(self, path):
        st_time = time.time()
        if os.path.exists(path) is False:
            self.send_text('/ERROR')
            return -1
        foldername = os.path.split(path)[-1]
        self.send_text(foldername)
        files = os.listdir(path)
        self.send_text(str(len(files)))
        for file in files:
            filepath = os.path.join(path, file)
            self.send_file(filepath)
            if self.recv_text() == '/ERROR':
                return -1
        end_time = time.time() - st_time
        return end_time

    def recv_folder(self, destination=None):
        st_time = time.time()
        foldername = self.recv_text()
        if foldername == '/ERROR':
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

    def set_info(self, info):
        self.username = info['username']
        self.password = info['password']

    def get_info(self):
        return {'username': self.username,
                'password': self.password,
                'isconnected': self.isconnected}

    def save_pw(self):
        pw_path = os.path.join(SERVER_PATH, self.username, PW)
        with open(pw_path, 'w') as f:
            f.write(json.dumps(self.get_info(), indent=4))
            f.close()


if __name__ == '__main__':
    # server = Server(sys.argv)
    IP = 'localhost'
    PORT = '23456'
    server = Server((None, IP, PORT))

    server.main()
