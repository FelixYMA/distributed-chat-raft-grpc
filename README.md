# Multiplayer Chat System

A distributed fault-tolerant chat system designed to run on a LAN network with multiple server instances using the RAFT consensus algorithm.  

- **Distributed Architecture**: Multiple server instances work together for fault tolerance
- **RAFT Consensus**: Automatic leader election and data replication
- **DNS Service**: Automatic service discovery
- **Data Replication**: Ensures message persistence across server failures
- **User-friendly GUI**: Clean modern interface for chat clients
- **Consistent Hashing**: For distributed data storage
- **MapReduce**: Analytics framework for processing chat data
- **Automatic Reconnection**: Clients automatically reconnect when servers fail

## System Architecture

The system consists of several components:

- **Chat Servers**: Handle client connections and messages
- **RAFT Protocol**: Ensures consistent data replication and leader election
- **DNS Service**: Provides service discovery for clients and servers
- **Data Store**: Distributed storage with consistent hashing
- **Client API**: Command-line and GUI interfaces for users

## Description of the contents in the README folder
- **Architecture.png**: The architecture of this project in Diagram
- **readme.md**: Introducing the project and how it works
- **report.pdf**: Describe the application and the distributed system capabilities
- **Running_Sample.mp4**: Video of a test run of this project (recommended viewing)
## Prerequisites

- Python 3.7 or higher
- Required Python packages:
  - grpcio
  - grpcio-tools
  - protobuf
  - tkinter (for GUI client)

## Installation

1. Clone the repository or download the source code
2. Install the required packages:

```bash
pip install grpcio grpcio-tools protobuf
```

3. Generate the gRPC code from the protobuf definition:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. chat.proto
```

## Running the System

### Quick Start (Windows)

For Windows users, a batch script is provided to start the entire system at once:

```bash
simple_start_3_server.bat
```

This script will:
1. Start a DNS service on port 50051
2. Start three chat server instances on ports 50050, 50060, and 50070
3. Optionally launch the GUI client

You can then run the client by simply opening MultiPlayer.exe. 

### Manual Setup

#### 1. Start the DNS Service

```bash
python dns_service.py --port 50051 --host 0.0.0.0
```

#### 2. Start the First Server

```bash
python server.py --id server1 --port 50050 --host 0.0.0.0 --dns <your_ip>:50051
```

#### 3. Start Additional Servers

```bash
python server.py --id server2 --port 50060 --host 0.0.0.0 --dns <your_ip>:50051 --peers server1=<your_ip>:50050
```

```bash
python server.py --id server3 --port 50070 --host 0.0.0.0 --dns <your_ip>:50051 --peers server1=<your_ip>:50050,server2=<your_ip>:50060
```
And so on, you can also start more servers.  
Note: Replace `<your_ip>` with your actual local IP address.

#### 4. Start the GUI Client

```bash
python client_gui.py
```

## Using the Chat Client

### GUI Client

1. Launch the client using:  
  the command above or   
  from the batch script or   
  run the client by simply opening MultiPlayer.exe(recommend).
2. Enter the server address and DNS service address (if different from default)
3. Click "Connect" to connect to the server
4. Enter your username
5. Click "Join Chat" to enter the chat room
6. Start chatting!

### Command-Line Client

A command-line client is also available:

```bash
python client.py --server <your_ip>:50070 --dns <your_ip>:50051
```

## Troubleshooting

- **Connection Issues**: Ensure your firewall allows the required ports
- **Server Startup Failures**: Check if the ports are already in use and verify that the server address is correct
- **Client Connection Problems**: Verify that the server address is correct
- **How to close a port manually on Windows systems:**  
1. Find the ID of the process occupying the port:  
```bash
	netstat -ano | findstr :50051  
```
2. This will show output similar to this:  
	TCP 127.0.0.1:50051 0.0.0.0:0 LISTENING 12345  
Last number (e.g. 12345) is the process ID (PID).  
3. Use the PID to terminate the process:  
```bash
	taskkill /PID 12345 /F 
``` 
The /F option forces the process to terminate.

## System Resilience

The system is designed to be fault-tolerant:

1. If a follower server fails, the system continues to operate normally
2. If the leader server fails, a new leader is automatically elected
3. Clients automatically detect leader changes and reconnect
4. Data is replicated across multiple servers for persistence

## Technical Details

### Consensus Protocol

The system uses the RAFT consensus algorithm for:
- Leader election
- Log replication
- Safety guarantees

### Communication Protocol

All communication is done via gRPC, which provides:
- Efficient binary serialization
- Streaming capabilities for real-time updates
- Service definitions via Protocol Buffers

### Data Distribution

Data is distributed using consistent hashing to:
- Balance data across servers
- Minimize data redistribution when servers join/leave
- Provide predictable data placement

## Architecture Diagram

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Server 1  │◄────►│   Server 2  │◄────►│   Server 3  │
│  (Follower) │      │  (Follower) │      │  (Leader)   │
└─────┬───────┘      └─────┬───────┘      └─────┬───────┘
      │                    │                    │
      │                    ▼                    │
      │            ┌─────────────┐              │
      └───────────►│ DNS Service │◄─────────────┘
                   └─────┬───────┘
                         │
                         ▼
               ┌─────────────────────┐
               │      Clients        │
               └─────────────────────┘
```

## Credits

This distributed chat system was developed as a demonstration of distributed systems concepts including consensus algorithms, service discovery, and fault tolerance.
