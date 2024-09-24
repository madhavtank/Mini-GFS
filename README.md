# Mini-GFS: Dockerized Python-based Distributed File System

Mini-GFS is a simplified implementation of the [Google File System (GFS)](http://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) built using Docker containers. This project aims to illustrate the core concepts of GFS, including distributed data storage, metadata management, and client-server interactions. By simulating GFS components within Docker containers, Mini-GFS provides a scalable and fault-tolerant distributed file system suitable for educational purposes and small-scale applications.
    
![minigfs](https://github.com/YashK2003/Mini-GFS/assets/102593613/ebfc90a6-6f2f-49b2-bacd-ec207cd989d8)

## System Overview
Mini-GFS consists of the following components:

1. **Master Server (`primary.py`)**: Manages file system metadata, coordinates chunk servers, and handles client requests.
2. **Secondary Server (`secondary.py`)**: Assumes the role of the master if it goes down until the master is restored.
3. **Chunk Server (`chunk_server.py`)**: Stores and retrieves data chunks, communicates with the master and other chunk servers.
4. **Client (`client.py`)**: Interfaces with the file system to perform file operations such as reading, writing, and appending.

## Code Structure and Functionality
### `primary.py`
- Manages file system metadata, chunk server coordination, and client requests.
- Implements threading classes for registration, heartbeat checks, and metadata updates.

### `secondary.py`
- Assumes the role of the master if it goes down until the master is restored.
- Synchronizes with the primary server to maintain system consistency and availability.


### `chunk_server.py`
- Handles requests from the master, clients, and other chunk servers.
- Implements operations such as read, write, append, and delete for data chunks.
- Ensures data consistency and coordination between chunk servers.

### `client.py`
- Connects to the master server to perform file operations based on user commands.
- Supports commands for writing, reading, appending, and deleting files.

## Features Implemented

**Master Server Features:**
- **Client Communication:** Interacts with client requests for file operations like read, write, append, and delete.
- **Chunk Server Registration:** Allows chunk servers to register themselves for data storage and retrieval.
- **File Metadata Management:** Tracks metadata related to files, including size, chunk distribution, and status.
- **Chunk Server Load Balancing:** Distributes chunks across available servers based on load to balance the system.
- **Heartbeat Mechanism:** Implements a heartbeat mechanism to check the health of chunk servers and ensure their availability.
- **Fault Tolerance:** Handles scenarios where chunk servers go down by redistributing chunks to other available servers.
- **Multi-threading:** Utilizes threads to handle concurrent client requests and server operations efficiently.
- **Persistent Metadata Storage:** Stores metadata about chunk servers and files persistently to handle server restarts.

**Client Features:**
- **Interfacing with Master:** Connects to the master server to perform file operations like read, write, append, and delete.
- **File Operations:** Supports commands for uploading, reading, appending, and deleting files.
- **Error Handling:** Provides error messages for invalid commands or failed operations.
- **User-Friendly Interface:** Guides users on how to use commands and interact with the file system effectively.
- **Connection Retry:** Attempts to connect to the backup server if the primary server is unavailable.

These features collectively enable distributed file storage and management in the Mini-GFS system, demonstrating the core concepts of a distributed file system.

## Running the Project
### Without Docker
1. Start the Master Server: `python3 primary.py`
2. Start the Secondary Server: `python3 secondary.py`
3. Start Chunk Servers: `python3 chunkserver.py <port_no.>`
4. Use the Client: `python3 client.py` and execute commands.

### With Docker
1. Build Docker images: `sudo docker compose build master_server secondary_server client chunk_server`
2. Run Master, Secondary, and Client Servers: `sudo docker compose run master_server`, `sudo docker compose run secondary_server`, `sudo docker compose run client`
3. Run Chunk Servers with specified port: `sudo docker compose run -e PORT=<port_no.> chunk_server`

## Client Commands
- `read <filename> <tofile>`: Retrieves a file and saves the data to the specified file.
- `write <filename> <data>`: Writes new data to a file.
- `append <tofile> <fromfile>`: Appends data to an existing file.
- `delete <filename>`: Deletes the specified file from the system.

## Contribution
Thanks to all the contributors to the above project:
- Chirag Jain
- Yash Kawade
- Kabir Shamlani
