
import socket
import os
import sys

MASTER_SV_PORT = 8080
MASTER_SV_BACKUP_PORT = 8081

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
RESET = '\033[0m'
YELLOW = '\033[93m'


def connect_to_master_server(inputtext):
    try:
        # Attempt to connect to the master server
        primary_server_address = ("127.0.0.1", MASTER_SV_PORT)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(primary_server_address)
        print(GREEN + "Connected to the master server."+ RESET)
    except ConnectionRefusedError:
        try:
            # If primary server connection fails, try backup server
            backup_server_address = (socket.gethostbyname('localhost'), MASTER_SV_BACKUP_PORT)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(backup_server_address)
            print(GREEN + "Connected to the backup server."+ RESET)
        except ConnectionRefusedError:
            # If both connections fail, print error message and exit
            print(RED + "Master server is not active. Please try again later."+ RESET)
            sys.exit()

    inputtype = inputtext.split(' ')[0]
    print(BLUE +"Connecting to master to get metadata"+ RESET)
    
    # For the write command
    try:
        if inputtype == "write":
            filename = inputtext.split(' ')[1]
            # Retrieve file size
            file_size = str(os.path.getsize(filename))
            # Prepare message with metadata
            metadata_message = f"client:write:{filename}:{file_size}"
            # Send metadata to the server
            s.send(metadata_message.encode("ascii"))
            # Receive acknowledgment from the server
            status = s.recv(32768).decode("ascii")
            # Close the connection
            s.close()
            print(GREEN + "Metadata successfully received."+ RESET)
            return status
    except Exception as e:
        print(RED + f"An error occurred: {e}"+ RESET)
        return None

    # For the read command -
    try:
        if inputtype == "read":
            filename = inputtext.split(' ')[1]
            # Prepare message to request file metadata
            file_request = f"client:read:{filename}"
            # Send file request to the server
            s.send(file_request.encode("ascii"))
            # Receive file metadata from the server
            status = s.recv(32768).decode("ascii")
            # Close the connection
            s.close()
            print(GREEN + "Metadata successfully received."+ RESET)
            return status
    except Exception as e:
        print(RED + f"An error occurred: {e}"+ RESET)
        return None
    
    # For the append command -
    try:
        if inputtype == "append":
            to_file = inputtext.split(' ')[1]
            from_file = inputtext.split(' ')[2]
            # Get the size of the file to be appended
            file_size = str(os.path.getsize(from_file))
            # Prepare message with metadata for appending
            metadata_message = f"client:append:{to_file}:{file_size}"
            # Send metadata to the server
            s.send(metadata_message.encode("ascii"))
            # Receive acknowledgment from the server
            status = s.recv(32768).decode("ascii")
            # Close the connection
            s.close()
            print(GREEN + "Metadata successfully received."+ RESET)
            return status
    except Exception as e:
        print(RED + f"An error occurred: {e}" + RESET)
        return None
    
    try:
        if inputtype == "delete":
            filename = inputtext.split(' ')[1]
            # Prepare message with metadata
            metadata_message = f"client:delete:{filename}"
            # Send metadata to the server
            s.send(metadata_message.encode("ascii"))
            # Receive acknowledgment from the server
            status = s.recv(32768).decode("ascii")
            # Close the connection
            s.close()
            print(GREEN + "Metadata successfully received."+ RESET)
            return status
    except Exception as e:
        print(RED + f"An error occurred: {e}"+ RESET)
        return None


#Connecting to the chunk_server
def send_to_chunk_server(inputtype,inputtext,chunkcheck):

    print(BLUE + "Connecting to respective chunk server" + RESET)
    words = inputtext.split()
    filenames = words[1:]

    print("Chunk check",chunkcheck)

    if(inputtype == "read"):
        try:
            filetransfer = inputtext.split(' ')[2]
            # print(filetransfer)
            with open(filetransfer, 'w') as file:
                for chunk_data in chunkcheck.split(";"):
                    filename_chunkid, ip_ports = chunk_data.split("=")
                    ip, port = ip_ports.split(",")[0].split(":")
                    port = int(port)

                    chunk_server_msg = f"client:read:{filename_chunkid}"
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((ip, port))
                        s.send(chunk_server_msg.encode('ascii'))
                        print(f"Reading {filename_chunkid}...")
                        status = s.recv(2048)
                        # print("status" , status.decode('ascii'))
                        file.write(status.decode('ascii'))
                    print(GREEN + "Chunk successfully read."+ RESET)
        except Exception as e:
            print(RED + f"An error occurred: {e}"+ RESET)
            return None

    if inputtype == "append" or inputtype == "write":

        try:
            if inputtype == "append":
                file_from = filenames[1] if filenames else None
            else:
                file_from = filenames[0] if filenames else None

            ip_port_arr = chunkcheck.split(",")
            chunk_path = "./" + file_from
            with open(chunk_path, 'rb') as f:

                for ip_port_size in ip_port_arr:
                    ip_port, write_file_size = ip_port_size.split("=")
                    ip, port = ip_port.split(":")

                    # print("write_file_size",write_file_size)

                    filename_chunkid, writeSize = write_file_size.split(":")
                    port = int(port)
                    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    s.connect((ip,port))
                    chunk_server_msg = "client:" + "append:" + filename_chunkid + ":" + writeSize
                    s.send(chunk_server_msg.encode('ascii'))
                    status = s.recv(100)
                    data=f.read(int(writeSize))
                    s.sendall(data)
                    s.close()
        except Exception as e:
            print(RED + f"An error occurred: {e}" + RESET)
            return None
        
        if(inputtype == "write"):
            print(GREEN + "File write successfull." + RESET)
        else:
            print(GREEN + "File appended successfully." + RESET)

    elif inputtype == "delete":
        try:

            for chunk_data in chunkcheck.split(";"):
                filename_chunkid, ip_ports = chunk_data.split("=")
                all_ip_ports = ip_ports.split(",")
                
                all_ip_ports = list(set(all_ip_ports))

                for x in all_ip_ports:
                    ip, port = x.split(":")
                    port = int(port)
                    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    s.connect((ip,port))
                    chunk_server_msg = "client:" + "delete:" + filename_chunkid
                    s.send(chunk_server_msg.encode('ascii'))
                    status = s.recv(100)
                    # print(status)
                    s.close()

        except Exception as e:
            print(RED + f"An error occurred: {e}" + RESET)
            return None
        
        print(GREEN + "File deleted successfull." + RESET)
    
    return

if __name__=="__main__":
    print(YELLOW + "Enter commands in the following format: " + RESET)
    print(YELLOW + "To upload a file: write <filename>" + RESET)
    print(YELLOW + "To read a file: read <filename> <transfer filename>" + RESET)
    print(YELLOW + "To append some data in a file: append <tofile> <fromfile>" + RESET)

    while True:
        inputtext  = input(YELLOW + ">> " + RESET)
        inputtype = inputtext.split(' ')[0]

        numberofargs = len(inputtext.split())

        if(inputtype != "write" and inputtype != "read" and inputtype != "append" and inputtype != "delete" and inputtype != "exit"):
            print(RED + "Invalid Command" + RESET)
            continue
            

        if inputtype == "exit":
            print(GREEN + "Exiting the client." + RESET)
            break

        if inputtype == "write" and numberofargs != 2:
            print(RED + "Invalid Command" + RESET)
            continue
        elif inputtype == "read" and numberofargs != 3:
            print(RED + "Invalid Command" + RESET)
            continue
        elif inputtype == "append" and numberofargs != 3:
            print(RED + "Invalid Command" + RESET)
        elif inputtype == "delete" and numberofargs != 2:
            print(RED + "Invalid Command" + RESET)
            continue

        chunkcheck = connect_to_master_server(inputtext)

        if chunkcheck==None:
            continue

        if chunkcheck.startswith('$error:'):
            print(RED + chunkcheck + RESET)
            continue


        if not chunkcheck and inputtype == "write":
            print(RED + "File already Present"+ RESET)
            continue
        elif not chunkcheck and inputtype == "read":
            print(RED + "File not Present"+ RESET)
            continue
        elif not chunkcheck and inputtype == "append":
            print(RED + "File not Appended"+ RESET)
            continue

        # if inputtype == "delete":
        #     print(GREEN + "File deleted successfully."+ RESET)
        #     continue

        send_to_chunk_server(inputtype, inputtext, chunkcheck)
        print()