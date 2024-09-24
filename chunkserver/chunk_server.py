import socket, threading
import os
import sys

MASTER_IP = "127.0.0.1"
MASTER_PORT = 8080
DUPLICATE_MASTER_IP = "127.0.0.1"
DUPLICATE_MASTER_PORT = 8081
MAX_CHUNK_SIZE = 2048

GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
RESET = '\033[0m'
YELLOW = '\033[93m'
GREY = '\033[90m'

class ChunkServer():
	mutual_excl = {}

	def __init__(self):


		PORT = None if len(sys.argv) < 2 else sys.argv[1]

		if PORT == None:
			PORT = os.environ.get("PORT", 6969)

		self.myport = int(PORT)

		print(BLUE + "Registering chunk server..." + RESET)

		def connect_to_master(ip, port):
			try:
				client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				client_socket.bind(("127.0.0.1", self.myport))
				client_socket.connect((ip, port))
				return client_socket
			except:
				return None

		client = connect_to_master(MASTER_IP, MASTER_PORT)
		if not client:
			client = connect_to_master(DUPLICATE_MASTER_IP, DUPLICATE_MASTER_PORT)
			if not client:
				sys.exit()

		self.path = "./" + str(self.myport)
		if os.path.exists(self.path):
			chunks = os.listdir(self.path)
		else:
			os.mkdir(self.path)
			chunks = []

		def get_chunk_info(chunk_path, chunks):
			chunk_info = ""
			for file_name in chunks:
				file_path = os.path.join(chunk_path, file_name)
				file_stats = os.stat(file_path)
				file_size = file_stats.st_size
				chunk_info += f"{file_name}:{file_size},"
			return chunk_info.rstrip(",")

		chunk_info = get_chunk_info(self.path, chunks)

		client.sendall("register".encode())
		client.recv(60)
		client.sendall(chunk_info.encode())
		print(GREEN + "Chunk server registered" + RESET)
		client.close()

	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(("127.0.0.1",self.myport))
		while True:
			sock.listen()
			client, address = sock.accept()
			threading.Thread(target = self.checkoperation,args = (client,address)).start()

	def checkoperation(self, client, address):
		recv = client.recv(400).decode("utf-8")
		tokens = recv.split(":")

		if tokens[0] == "master":
			self.handle_master_request(tokens, client, recv)
		elif tokens[0] == "client":
			self.handle_client_request(tokens, client, address)
		elif tokens[0] == "chunkserver":
			self.handle_chunkserver_request(tokens, client, address)

	def handle_master_request(self, tokens, client, recv):
		if tokens[1] == "heartbeat":
			self.heartbeat_reply(client)
		elif tokens[1] == "copy":
			client.close()
			self.copyfromchunkserver(recv[12:])

	def handle_client_request(self, tokens, client, address):
		if tokens[1] == "read":
			self.sendchunk(tokens, client, address)
		elif tokens[1] == "append":
			self.handle_append_request(tokens, client)
		elif tokens[1] == "write":
			self.handle_write_request(tokens, client)
		elif tokens[1] == "delete":
			self.handle_delete_request(tokens, client)

	def handle_append_request(self, tokens, client):
		chunkid = tokens[2]
		if chunkid in self.mutual_excl:
			mutual = [tokens, client]
			self.mutual_excl[chunkid].append(mutual)
		else:
			self.appendchunk(tokens, client)

	def handle_write_request(self, tokens, client):
		chunkid = tokens[2]
		if len(self.mutual_excl[chunkid]) != 0:
			mutual = [tokens, client]
			self.mutual_excl[chunkid].append(mutual)
		else:
			self.appendchunk(tokens, client)

	def handle_delete_request(self, tokens, client):
		# chunkid = tokens[2]
		# if len(self.mutual_excl[chunkid]) != 0:
		# 	mutual = [tokens, client]
		# 	self.mutual_excl[chunkid].append(mutual)
		# else:
		self.deletechunk(tokens, client)

	def handle_chunkserver_request(self, tokens, client, address):
		if tokens[1] == "appendinfo":
			self.handle_appendinfo_request(tokens, client)
		elif tokens[1] == "sendcopy":
			self.sendchunk(tokens, client, address)

	def handle_appendinfo_request(self, tokens, client):
		chunkid = tokens[2]
		if chunkid in self.mutual_excl:
			mutual = [tokens, client]
			self.mutual_excl[chunkid].append(mutual)
		else:
			self.appendchunk(tokens, client)
	
	def heartbeat_reply(self,client):
		print(GREY + "Replying to heartbeat msg" + RESET)
		client.sendall("ok".encode())
		client.close()

	def copyfromchunkserver(self,copylist):
		print(RED + "Copying chunk from chunkserver..." + RESET)
		copylist = copylist.split(',')

		for item in copylist:

			item = item.split('=')
			chunkname = item[1]
			item = item[0].split(":")
			serverip, serverport  = item[0],item[1]
			tosend = "chunkserver:sendcopy:"+chunkname
			s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			s1.connect((serverip, int(serverport)))
			s1.sendall(tosend.encode())
			chunk = self.path+"/"+chunkname
			with open(chunk,'wb') as f1:
				data = s1.recv(MAX_CHUNK_SIZE)
				f1.write(data)
			s1.close()
		print(GREEN + "chunk Recieved" + RESET)

	def sendchunk(self,to_recv,client,address):
		print(BLUE + "Sending chunk "+to_recv[2] + " ..." + RESET)
		chunk=self.path+"/"+to_recv[2]
		with open(chunk, 'rb') as f:
			data=f.read(MAX_CHUNK_SIZE)
			client.sendall(data)
		print(GREEN + "Chunk sent" + RESET)
		client.close()
		
	def appendchunk(self, recv, client_con):

		print(BLUE + f"Appending data to chunk {recv[2]}..." + RESET)
		self.mutual_excl[recv[2]] = []
		mutual = [recv, client_con]
		self.mutual_excl[recv[2]].append(mutual)

		while len(self.mutual_excl[recv[2]]) != 0:
			conn = self.mutual_excl[recv[2]][0]
			to_recv, client = conn[0], conn[1]
			chunk = os.path.join(self.path, to_recv[2])
			size_to_append = int(to_recv[3])
			client.sendall(b"ok")

			with open(chunk, 'ab') as f:
				data = client.recv(size_to_append)
				f.write(data)

			client.close()
			self.mutual_excl[recv[2]].pop(0)

		# client_con.close()

			print(BLUE + "Updating info" + RESET)
			self.update_master_info()

			if to_recv[0] == "client":
				print(GREEN + "Data appended to primary replica" + RESET)
				self.sendtosecondary(data, size_to_append, to_recv[2])
				print(GREEN + "Data appended to all replicas" + RESET)

		del self.mutual_excl[recv[2]]

	def deletechunk(self, recv, client_con):
		print(BLUE + f"Deleting chunk {recv[2]}..." + RESET)
		chunk = os.path.join(self.path, recv[2])
		os.remove(chunk)
		print(GREEN + "Chunk deleted" + RESET)
		client_con.close()

	def update_master_info(self):
		try:
			s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s1.connect((MASTER_IP, MASTER_PORT))
		except:
			try:
				s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s1.connect((DUPLICATE_MASTER_IP, DUPLICATE_MASTER_PORT))
			except:
				sys.exit()

		chunks = os.listdir(self.path)
		msg_to_send = ""
		msg = "update:127.0.0.1:" + str(self.myport)
		s1.sendall(msg.encode())

		for file in chunks:
			filename = os.path.join(self.path, file)
			file_stats = os.stat(filename)
			curr_size = file_stats.st_size
			msg_to_send += f"{file}:{curr_size},"

		if len(chunks) != 0:
			msg_to_send = msg_to_send[:-1]

		s1.recv(60)  # Receive acknowledgment
		s1.sendall(msg_to_send.encode())
		s1.close()
		print(GREEN + "info updated" + RESET)

	def sendtosecondary(self, data, sizetoappend, file):
		print(BLUE + "Copying to secondary replicas" + RESET)

		# Try connecting to the primary and secondary masters
		for master_ip, master_port in [(MASTER_IP, MASTER_PORT), (DUPLICATE_MASTER_IP, DUPLICATE_MASTER_PORT)]:
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
					s1.connect((master_ip, master_port))
					s1.sendall(("info:" + file).encode())
					print(GREEN + "info updated" + RESET)
					getlist = s1.recv(MAX_CHUNK_SIZE).decode().split(',')

					break
			except:
				pass
		else:
			sys.exit()

		# Connect to secondary replicas and send data
		for i, item in enumerate(getlist, start=1):
			if item:
				serverip, serverport = item.split(':')
				if int(serverport) != self.myport:
					try:
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
							s1.connect((serverip, int(serverport)))
							tosend = f"chunkserver:appendinfo:{file}:{sizetoappend}"
							s1.sendall(tosend.encode())
							s1.recv(1024)  # Receive acknowledgment
							s1.sendall(data)
						print(GREEN + f"Copied to secondary replica {i}" + RESET)
						if i >= 3:
							break
					except:
						pass

master = ChunkServer()	
master.run()