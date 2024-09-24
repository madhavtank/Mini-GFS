import os
import math
import socket
import pickle
import operator
import threading
from collections import namedtuple

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
RESET = '\033[0m'
YELLOW = '\033[93m'
GREY = '\033[90m'

HOST = "127.0.0.1"
PRIMARY_PORT = 8080
SECONDARY_PORT = 8081

MAXSIZE = 2048

chunkservers = {}
files = {}

class ChunkServer:

    def __init__(self, ip, port, status):
        self.ip = ip
        self.port = port
        self.status = status
        self.load = 0
        self.chunk_info = {}

    def add_chunk(self, chunk, size):
        self.chunk_info[chunk] = size
        self.load += 1

    def get_status(self):
        return self.status

    def set_status(self, status):
        self.status = status

    def get_IP(self):
        return self.ip

    def get_port(self):
        return self.port

    def get_chunks(self):
        return self.chunk_info.keys()

    def update_chunk(self, chunk_list):

        chunks = chunk_list.split(',')

        for cl in chunks:
            c = cl.split(':')
            self.add_chunk(c[0], int(c[1]))


class FileInfo:

    def __init__(self, name, total_size):
        self.name = name
        self.total_size = int(total_size)
        self.chunk_info = {}
        self.has_last_chunk = False
        self.last_chunk_ID = None
        self.total_chunks = math.ceil(self.total_size/MAXSIZE)
        self.update_last_chunkstatus()

    def update_last_chunkstatus(self):
        self.has_last_chunk = (self.total_size%MAXSIZE != 0)
        self.last_chunk_ID = self.name + "_" + str(math.ceil(self.total_size/MAXSIZE))

    def update_file_size(self, size):
        self.total_size += size
        self.update_last_chunkstatus()

    def update_chunk_info(self, chunk, cs):
        global chunkservers
        if chunk not in self.chunk_info.keys():
            self.chunk_info[chunk] = []
        self.chunk_info[chunk].append(chunkservers[cs])

    def get_last_chunk_status(self):
        return self.has_last_chunk

    def get_total_size(self):
        return self.total_size

    def get_total_chunks(self):
        return self.total_chunks

    def get_last_chunk_ID(self):
        return self.last_chunk_ID

    def get_chunk_info(self, chunkID):
        if chunkID not in self.chunk_info.keys():
            return []
        return self.chunk_info[chunkID]

    def get_all_chunk_info(self):
        return self.chunk_info

    def get_fist_chunk_server(self, chunkID):
        return self.chunk_info[chunkID][0]

    def remove_server_info(self, chunk, cs):
        i = 0

        for obj in self.chunk_info[chunk]:
            if cs[0]==obj.get_IP() and cs[1]==obj.get_port():
                break
            i += 1

        self.chunk_info[chunk].pop(i)

class MetadataManager:
    def __init__(self, chunkservers_file='chunkservers.meta', files_file='files.meta'):
        self.chunkservers_file = chunkservers_file
        self.files_file = files_file

    def get_file_name(self, name):
        return name.split('_')[0]

    def write_metadata(self):

        with open(self.chunkservers_file, 'wb') as output:
            pickle.dump(chunkservers, output, pickle.HIGHEST_PROTOCOL)

        with open(self.files_file, 'wb') as output:
            pickle.dump(files, output, pickle.HIGHEST_PROTOCOL)

    def read_metadata(self):
        global chunkservers
        global files

        with open(self.chunkservers_file, 'rb') as ip:
            chunkservers = pickle.load(ip)

        with open(self.files_file,'rb') as ip:
            files = pickle.load(ip)

metadata_manager = MetadataManager()
if os.path.exists('chunkservers.meta') and os.path.exists('files.meta'):
    metadata_manager.read_metadata()

class ClientThread(threading.Thread):
    def __init__(self, client_address, client_socket, info):
        super().__init__()
        self.chunk_address = client_address
        self.chunk_socket = client_socket
        self.info = info

    def run(self):
        print(BLUE + "Client Connected: ", self.chunk_address , RESET)

        global chunkservers, files
        files_message = ''
        operation = self.info[0]
        
        if operation == 'read':
            files_message = self.read_file(self.info[1])
        elif operation == 'write':
            files_message = self.write_file(self.info[1], int(self.info[2]))
        elif operation == 'append':
            files_message = self.append_file(self.info[1], int(self.info[2]))
        elif operation == 'delete':
            files_message = self.delete_file(self.info[1])

        self.chunk_socket.sendall(bytes(files_message, 'UTF-8'))
        self.chunk_socket.close()

    def read_file(self, name):
        global files
        if name not in files:
            return "$error: file not found"
        obj = files[name]
        chunk_info = obj.get_all_chunk_info()
        message = ''

        for chunk, servers in chunk_info.items():
            server_list = ','.join([f"{cs.get_IP()}:{cs.get_port()}" for cs in servers])
            message += f"{chunk}={server_list};"

        return message[:-1]

    def write_file(self, name, size):
        global chunkservers, files
        obj = FileInfo(name, size)
        files[name] = obj
        chunk_server_list = sorted(chunkservers.values(), key=operator.attrgetter('load'))
        message = ''
        cs_count = len(chunk_server_list)

        for i in range(obj.get_total_chunks()):
            ip = chunk_server_list[i % cs_count].get_IP()
            port = chunk_server_list[i % cs_count].get_port()
            write_size = MAXSIZE if i < obj.get_total_chunks() - 1 else size % MAXSIZE

            message += f"{ip}:{port}={name}_{i+1}:{write_size},"

        return message[:-1]

    def delete_file(self, name):
        global files
        if name not in files:
            return "$error: file not found"
        obj = files[name]
        chunk_info = obj.get_all_chunk_info()
        message = ''

        for chunk, servers in chunk_info.items():
            server_list = ','.join([f"{cs.get_IP()}:{cs.get_port()}" for cs in servers])
            message += f"{chunk}={server_list};"

        del files[name]

        return message[:-1]

    def append_file(self, name, size):
        global chunkservers, files
        if name not in files:
            return "$error: file not found"
        obj = files[name]
        new_size = size
        chunk_server_list = sorted(chunkservers.values(), key=operator.attrgetter('load'))
        message = ''

        if obj.get_last_chunk_status():
            message += self.append_to_existing_chunk(obj, new_size, chunk_server_list, name)
        else:
            message += self.create_new_chunks(obj, new_size, chunk_server_list, name)

        obj.update_file_size(size)
        return message[:-1]

    def append_to_existing_chunk(self, obj, new_size, chunk_server_list, name):
        last_chunk_id = obj.get_last_chunk_ID()
        old_size = obj.get_total_size()
        last_chunk_size = old_size % MAXSIZE
        to_add = MAXSIZE - last_chunk_size
        chunk_server_info = obj.get_chunk_info(last_chunk_id)
        ip, port = chunk_server_info[0].get_IP(), chunk_server_info[1].get_port()

        if new_size <= to_add:
            return f"{ip}:{port}={last_chunk_id}:{new_size},"
        else:
            return f"{ip}:{port}={last_chunk_id}:{to_add}," + self.create_new_chunks(obj, new_size - to_add, chunk_server_list, name)

    def create_new_chunks(self, obj, new_size, chunk_server_list, name):
        j = 0
        last_chunk_number = 1 if not obj.get_last_chunk_status() else int(obj.get_last_chunk_ID().split('_')[1]) + 1
        total_chunks = math.ceil(new_size / MAXSIZE)
        message = ''

        print("------------> " , new_size, total_chunks, last_chunk_number)


        for chunk_index in range(total_chunks):
            ip = chunk_server_list[j].get_IP()
            port = chunk_server_list[j].get_port()
            write_size = MAXSIZE if chunk_index < total_chunks - 1 else new_size % MAXSIZE

            chunk_id = f"{name}_{last_chunk_number}"
            # obj.chunks[chunk_id] = chunk_server_list[j]
            message += f"{ip}:{port}={chunk_id}:{write_size},"
            last_chunk_number += 1
            j = (j + 1) % len(chunk_server_list)

        return message

class HeartbeatThread(threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        self.check_chunkservers()
        print(GREY + "Heartbeat Round Over. Waiting for 20 seconds..." + RESET)
        threading.Timer(20, self.run).start()

    def check_chunkservers(self):

        global chunkservers
        global files
        
        for cs_addr, chunkserver in list(chunkservers.items()):
            if chunkserver.get_status():
                ip, port = cs_addr
                if not self.send_heartbeat(ip, port):
                    print(RED + f"{cs_addr} chunkserver is down, copying chunks to other chunkserver" + RESET)
                    self.handle_chunkserver_down(cs_addr)

    def send_heartbeat(self, ip, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as heartbeat:
                heartbeat.connect((ip, port))
                heartbeat.sendall(b"master:heartbeat")
                heartbeat.recv(2048)  # Receive response
            return True
        except socket.error:
            return False

    def handle_chunkserver_down(self, cs_addr):

        global chunkservers
        global files
        
        chunks_to_copy = chunkservers[cs_addr].get_chunks()
        message_parts = ["master:copy:"]

        for chunk in chunks_to_copy:
            file_name = metadata_manager.get_file_name(chunk)
            file_obj = files[file_name]
            file_obj.remove_server_info(chunk, cs_addr)
            copy_server = file_obj.get_fist_chunk_server(chunk)
            message_parts.append(f"{copy_server.get_IP()}:{copy_server.get_port()}={chunk},")

        message = ''.join(message_parts)[:-1]  # Remove trailing comma
        chunkservers[cs_addr].set_status(False)
        del chunkservers[cs_addr]

        self.send_copy_request(message)
        metadata_manager.write_metadata()

    def send_copy_request(self, message):

        global chunkservers
        global files

        chunk_server_list = list(chunkservers.values())
        chunk_server_list.sort(key=operator.attrgetter('load'))
        cs = chunk_server_list[0]  # Get the chunkserver with the lowest load
        send_ip, send_port = cs.get_IP(), cs.get_port()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
            sender.connect((send_ip, send_port))
            sender.sendall(message.encode())


class RegisterChunkServerThread(threading.Thread):
    def __init__(self, chunk_address, chunk_socket):
        super().__init__()
        self.chunk_socket = chunk_socket
        self.chunk_address = chunk_address

    def run(self):
        
        global chunkservers
        global files

        print(BLUE + "RegisterChunkServerThread Begins" + RESET)
        self.chunk_socket.sendall(b"ok")
        data = self.chunk_socket.recv(20000).decode()

        if data:
            self.handle_existing_chunks(data)
        else:
            self.register_new_chunkserver()

        self.chunk_socket.close()
        print(GREEN + "RegisterChunkServerThread Completed" + RESET)

    def handle_existing_chunks(self, data):
        
        global chunkservers
        global files

        ip, port = self.chunk_address
        obj = ChunkServer(ip, port, True)
        
        for chunk_info in data.split(','):
            chunk_name, chunk_id = chunk_info.split(':')
            obj.add_chunk(chunk_name, int(chunk_id))

        chunkservers[self.chunk_address] = obj

        for chunk_info in data.split(','):
            chunk_name, chunk_id = chunk_info.split(':')
            file_name = metadata_manager.get_file_name(chunk_name)
            file_obj = files[file_name]
            file_obj.update_chunk_info(chunk_name, self.chunk_address)

    def register_new_chunkserver(self):
        
        global chunkservers
        global files

        ip, port = self.chunk_address
        obj = ChunkServer(ip, port, True)
        chunkservers[self.chunk_address] = obj

class InfoThread(threading.Thread):

    def __init__(self, chunk_address, chunk_socket, chunk_name):
        threading.Thread.__init__(self)
        self.chunk_socket = chunk_socket
        self.chunk_address = chunk_address
        self.cname = chunk_name

    def run(self):
        print(BLUE + "InfoThread Begins" + RESET)

        global chunkservers
        global files

        file_name = metadata_manager.get_file_name(self.cname)
        file_obj = files[file_name]
        chunk_server_info = file_obj.get_chunk_info(self.cname)
        available_chunk_servers = [cs for cs in chunkservers.values() if cs not in chunk_server_info]
        available_chunk_servers.sort(key=operator.attrgetter('load'))
        chunk_server_info.extend(available_chunk_servers[:3 - len(chunk_server_info)])

        message = ','.join(f"{obj.get_IP()}:{obj.get_port()}" for obj in chunk_server_info)
        self.chunk_socket.sendall(bytes(message, 'UTF-8'))
        self.chunk_socket.close()
        print(GREEN + "InfoThread Completed" + RESET)

class UpdateThread(threading.Thread):

    def __init__(self, address, sock, ip, port):
        threading.Thread.__init__(self)
        self.chunk_address = address
        self.chunk_socket = sock
        self.chunk_ip = ip
        self.chunk_port = port

    def run(self):
        print(BLUE + "UpdateThread Begins" + RESET)

        global chunkservers
        global files

        chunk_server = (self.chunk_ip, int(self.chunk_port))
        self.chunk_socket.sendall(b"ok")
        data = self.chunk_socket.recv(2048).decode()
        chunkservers[chunk_server].update_chunk(data)

        chunk_info = namedtuple('chunk_info', ['chunk_name', 'file_name'])
        chunk_infos = [chunk_info(*chunk.split(':')) for chunk in data.split(',')]

        for chunk_info in chunk_infos:
            file_obj = files[metadata_manager.get_file_name(chunk_info.chunk_name)]
            file_obj.update_chunk_info(chunk_info.chunk_name, chunk_server)

        print(GREEN + "UpdateThread Finished" + RESET)
        metadata_manager.write_metadata()

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PRIMARY_PORT))
    print("Primary Server started")
    print("Waiting for requests...")

    HeartbeatThread().start()

    while True:
        server.listen()
        sock, address = server.accept()
        data = sock.recv(2048)
        message = data.decode()

        if message == 'register':
            RegisterChunkServerThread(address, sock).start()
        elif message == 'healthcheck':
            sock.sendall(b"ok")
        else:
            words = message.split(':')
            if words[0] == 'info':
                InfoThread(address, sock, words[1]).start()
            elif words[0] == 'client':
                ClientThread(address, sock, words[1:]).start()
            elif words[0] == 'update':
                UpdateThread(address, sock, words[1], words[2]).start()

main()