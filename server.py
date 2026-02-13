import logging
import threading
import time
import uuid
import json
import os
import grpc
import argparse
from concurrent import futures

# Import generated gRPC code (will be generated from chat.proto)
from proto import chat_pb2
from proto import chat_pb2_grpc

# Import our components
from raft import RaftNode, ServerState
from consistent_hash import ConsistentHash
from data_store import DataStore
from map_reduce import MapReduce
from dns_service import DNSService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_local_ip():
    """Get the IP address of the local machine that can be used for external connections"""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No need to actually connect
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


class ChatServer:
    """
    Main chat server implementation that integrates all distributed system components.
    """

    def __init__(self, server_id, host, port, peer_addresses=None, dns_address=None):
        """
        Initialize the chat server.

        Args:
            server_id: Unique identifier for this server
            host: Host address to bind to
            port: Port to listen on
            peer_addresses: Dictionary mapping server IDs to (host, port) tuples
            dns_address: Address of the DNS service
        """
        self.server_id = server_id
        self.host = host
        self.port = port
        self.peer_addresses = peer_addresses or {}
        self.dns_address = dns_address

        # Room information
        self.rooms = {}  # room_id -> {name, description, created_at}

        # Connected clients
        self.clients = {}  # user_id -> {username, room_id, context}

        # Locks
        self.clients_lock = threading.RLock()

        # Initialize components
        self._init_components()

        # gRPC server
        self.grpc_server = None

        logger.info(f"Chat server {server_id} initialized at {host}:{port}")

    def _message_handler(self, message, exclude_user_id=None):
        """
        Process a message and broadcast it to all clients in the room.

        Args:
            message: ChatMessage object
            exclude_user_id: Optional, user ID to exclude (don't send the message to this user)
        """
        # First store the message in the data store
        self.data_store.store_chat_message(message)

        # Find all online clients in the room
        with self.clients_lock:
            room_clients = {
                user_id: client_info
                for user_id, client_info in self.clients.items()
                if client_info['room_id'] == message.room_id
            }

        # Log that we're about to broadcast the message
        logger.info(
            f"Broadcasting message {message.message_id} to {len(room_clients)} clients in room {message.room_id}")

        # We don't actually send the message directly here
        # Messages will be sent to clients through the yield mechanism in the JoinChat method
        # This method only stores the message for future sending via stream

        # If you need to save the message somewhere for JoinChat to use, add code here
        # For example, you could add the message to a global message queue that JoinChat will pull from

        # Log that the message has been processed
        logger.info(f"Message {message.message_id} processed and stored")

        return True

    def _init_components(self):
        """Initialize all distributed system components."""
        # RAFT for leader election
        self.raft = RaftNode(
            server_id=self.server_id,
            peer_addresses=self.peer_addresses,
            port=self.port + 100  # Use a different port for RAFT
        )
        self.raft.on_leader_change = self._handle_leader_change

        # Data store with consistent hashing
        self.data_store = DataStore(
            server_id=self.server_id,
            peer_addresses=self.peer_addresses,
            replication_factor=min(3, len(self.peer_addresses) + 1),
            data_dir=f"./data/{self.server_id}"
        )

        # MapReduce for analytics
        self.map_reduce = MapReduce(
            data_store=self.data_store,
            worker_count=4,
            is_leader=False  # Will be updated when leader changes
        )

        # DNS client if DNS service is available
        self.dns_client = None
        if self.dns_address:
            self._init_dns_client()

    def _init_dns_client(self):
        """Initialize the DNS client."""
        try:
            channel = grpc.insecure_channel(self.dns_address)
            self.dns_client = chat_pb2_grpc.DNSServiceStub(channel)

            # Use actual IP for registration
            actual_ip = get_local_ip()

            # Register with DNS
            response = self.dns_client.Register(chat_pb2.RegisterRequest(
                server_name=self.server_id,
                address=actual_ip,  # Use actual IP instead of self.host
                port=self.port,
                is_leader=(self.raft.state == ServerState.LEADER)
            ))

            if response.success:
                logger.info(f"Registered with DNS at {self.dns_address}")
            else:
                logger.error(f"Failed to register with DNS: {response.error}")

        except Exception as e:
            logger.error(f"Error initializing DNS client: {e}")

    def _handle_leader_change(self, new_leader_id):
        """
        Handle leadership changes in the RAFT cluster.

        Args:
            new_leader_id: ID of the new leader
        """
        logger.info(f"New leader elected: {new_leader_id}")

        # Update MapReduce leader status
        self.map_reduce.is_leader = (new_leader_id == self.server_id)

        # Update DNS registration if we're the leader
        if self.dns_client and new_leader_id == self.server_id:
            try:
                response = self.dns_client.Register(chat_pb2.RegisterRequest(
                    server_name=self.server_id,
                    address=self.host,
                    port=self.port,
                    is_leader=True
                ))

                if not response.success:
                    logger.error(f"Failed to update leader status in DNS: {response.error}")

            except Exception as e:
                logger.error(f"Error updating leader status in DNS: {e}")

        # Notify connected clients about the new leader
        self._broadcast_server_status()

    def _broadcast_server_status(self):
        """Broadcast server status to all connected clients."""
        with self.clients_lock:
            for user_id, client_info in self.clients.items():
                try:
                    # Create a server status message
                    status = chat_pb2.ServerStatus(
                        server_id=self.server_id,
                        role=self.raft.state.name,
                        leader_id=self.raft.leader_id or "",
                        term=self.raft.current_term,
                        peer_servers=list(self.peer_addresses.keys())
                    )

                    # Send via client's context
                    client_info['context'].send(status)

                except Exception as e:
                    logger.error(f"Error sending status to client {user_id}: {e}")

    def start(self):
        """Start the chat server."""
        # Start RAFT
        self.raft_server = self.raft.start_server()

        # Start gRPC server
        self.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

        # Add servicers
        chat_pb2_grpc.add_ChatServiceServicer_to_server(
            self.ChatServicer(self), self.grpc_server
        )

        # Start server
        self.grpc_server.add_insecure_port(f"{self.host}:{self.port}")
        self.grpc_server.start()

        logger.info(f"Chat server started on {self.host}:{self.port}")

        # Start heartbeat to DNS
        if self.dns_client:
            threading.Thread(target=self._dns_heartbeat_loop).start()

        return self.grpc_server

    def _dns_heartbeat_loop(self):
        """Send periodic heartbeats to DNS service."""
        while True:
            try:
                if self.dns_client:
                    # Register again as a heartbeat
                    self.dns_client.Register(chat_pb2.RegisterRequest(
                        server_name=self.server_id,
                        address=self.host,
                        port=self.port,
                        is_leader=(self.raft.state == ServerState.LEADER)
                    ))
            except Exception as e:
                logger.error(f"Error sending DNS heartbeat: {e}")

            time.sleep(5)  # Heartbeat every 5 seconds

    def wait_for_termination(self):
        """Wait for server termination."""
        try:
            while True:
                time.sleep(86400)  # Sleep for a day
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop the chat server."""
        if self.grpc_server:
            self.grpc_server.stop(0)

        if self.raft_server:
            self.raft_server.stop(0)

        logger.info("Chat server stopped")

    def create_room(self, room_id, name, description):
        """
        Create a new chat room.

        Args:
            room_id: Unique room identifier
            name: Room name
            description: Room description

        Returns:
            True if successful
        """
        # Create room info
        room_info = {
            'room_id': room_id,
            'name': name,
            'description': description,
            'created_at': time.time(),
            'created_by': self.server_id
        }

        # Store in data store
        success = self.data_store.put('rooms', room_id, room_info)

        if success:
            self.rooms[room_id] = room_info
            logger.info(f"Created room {name} ({room_id})")

        return success

    def join_room(self, user_id, username, room_id, context):
        """
        Add a user to a room.

        Args:
            user_id: User identifier
            username: User's display name
            room_id: Room to join
            context: gRPC stream context for sending messages

        Returns:
            True if successful
        """
        with self.clients_lock:
            # Check if room exists, create default room if it doesn't exist and is the default room
            room_info = self.data_store.get('rooms', room_id)

            if not room_info and room_id == 'default':
                # Create default room
                self.create_room('default', 'General Chat', 'Default chat room')
                room_info = self.data_store.get('rooms', room_id)
                logger.info(f"Created missing default room on demand")

            if not room_info:
                logger.warning(f"User {username} tried to join non-existent room {room_id}")
                return False

            # Add user to client list, including message handler
            self.clients[user_id] = {
                'username': username,
                'room_id': room_id,
                'context': context,
                'joined_at': time.time(),
                'message_handler': lambda msg: None  # This is just a placeholder, actual handling in JoinChat
            }

            logger.info(f"User {username} ({user_id}) joined room {room_id}")



            return True

    def leave_room(self, user_id):
        """
        Remove a user from a room.

        Args:
            user_id: User identifier

        Returns:
            True if successful
        """
        with self.clients_lock:
            if user_id not in self.clients:
                return False

            client_info = self.clients[user_id]
            room_id = client_info['room_id']
            username = client_info['username']

            # Remove from clients
            del self.clients[user_id]

            logger.info(f"User {username} ({user_id}) left room {room_id}")

            # Send leave message to room
            self._send_system_message(
                room_id=room_id,
                content=f"{username} has left the room"
            )

            return True

    def send_message(self, message):
        """
        Process and deliver a chat message.

        Args:
            message: ChatMessage object

        Returns:
            True if successful
        """
        # Store message in data store
        self.data_store.store_chat_message(message)

        # Send to all clients in the room
        self._broadcast_message(message)

        return True

    def _broadcast_message(self, message):
        """
        Broadcast a message to all clients in a room.

        Args:
            message: ChatMessage object
        """
        with self.clients_lock:
            # Get all clients in the room where the message is sent
            room_clients = [
                (user_id, client_info) for user_id, client_info in self.clients.items()
                if client_info['room_id'] == message.room_id
            ]

            logger.info(
                f"Broadcasting message from {message.username} to {len(room_clients)} clients in room {message.room_id}")

            # Send message to each client
            for user_id, client_info in room_clients:
                try:
                    if 'message_handler' in client_info:
                        handler = client_info['message_handler']
                        if callable(handler):
                            logger.info(f"Sending message to client {user_id}")
                            handler(message)  # Directly call the message handler function
                        else:
                            logger.warning(f"Message handler for user {user_id} is not callable")
                    else:
                        logger.warning(f"Client {user_id} has no message handler")
                except Exception as e:
                    logger.error(f"Error sending message to client {user_id}: {e}")
                    continue

    def _send_system_message(self, room_id, content):
        """
        Send a system message to a room.

        Args:
            room_id: Room to send to
            content: Message content
        """
        # Create system message
        message = chat_pb2.ChatMessage(
            user_id="system",
            username="System",
            content=content,
            room_id=room_id,
            timestamp=int(time.time()),
            message_id=str(uuid.uuid4())
        )

        # Store and broadcast
        self.send_message(message)

    def get_room_messages(self, room_id, limit=100):
        """
        Get recent messages for a room.

        Args:
            room_id: Room identifier
            limit: Maximum number of messages

        Returns:
            List of messages
        """
        return self.data_store.get_chat_messages(room_id, limit)

    def get_rooms(self):
        """
        Get all available chat rooms.

        Returns:
            Dictionary of room_id -> room info
        """
        return self.data_store.get_all('rooms')

    def get_server_status(self):
        """
        Get current server status.

        Returns:
            Dictionary with server status
        """
        raft_status = self.raft.get_status()

        return {
            'server_id': self.server_id,
            'address': f"{self.host}:{self.port}",
            'raft_status': raft_status,
            'connected_clients': len(self.clients),
            'rooms': len(self.rooms)
        }

    def run_analytics(self, analytics_type):
        """
        Run analytics on chat data.

        Args:
            analytics_type: Type of analytics to run

        Returns:
            Job ID for tracking
        """
        if analytics_type == 'chat_analysis':
            return self.map_reduce.analyze_chat_data()
        else:
            logger.warning(f"Unknown analytics type: {analytics_type}")
            return None

    def get_analytics_results(self, job_id):
        """
        Get results of an analytics job.

        Args:
            job_id: Job identifier

        Returns:
            Results dictionary or None if not completed
        """
        return self.map_reduce.get_job_results(job_id)

    class ChatServicer(chat_pb2_grpc.ChatServiceServicer):
        """gRPC servicer for chat service."""

        def __init__(self, server):
            self.server = server

        def Ping(self, request, context):
            return chat_pb2.PingResponse(success=True, message="pong")

        def JoinChat(self, request, context):
            """Handle client joining a chat room."""
            user_id = request.user_id
            username = request.username

            # Use default room (if not specified)
            room_id = getattr(request, 'room_id', 'default')

            # Check if we are the leader
            if self.server.raft.state != ServerState.LEADER:
                # Redirect to leader
                if self.server.raft.leader_id:
                    leader_info = self.server.peer_addresses.get(
                        self.server.raft.leader_id
                    )

                    if leader_info:
                        host, port = leader_info
                        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                        context.set_details(f"Not the leader. Redirect to {host}:{port}")
                        return

                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Not the leader. No leader known.")
                return

            # Ensure default room exists before joining
            if room_id == 'default' and not self.server.data_store.get('rooms', 'default', local_only=True):
                self.server.create_room('default', 'General Chat', 'Default chat room')
                logger.info("Created missing default room before join")

            # Create message queue to store new messages
            message_queue = []
            message_condition = threading.Condition()

            # Create message handler function - this will be called by _broadcast_message
            def message_handler(msg):
                with message_condition:
                    message_queue.append(msg)
                    logger.info(f"Message from {msg.username} queued for user {user_id}: {msg.content[:30]}...")
                    message_condition.notify()

            # Join room, passing context
            success = self.server.join_room(user_id, username, room_id, context)

            if not success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Room {room_id} not found")
                return

            # Set client's message handler - ensure properly registered in client list
            with self.server.clients_lock:
                if user_id in self.server.clients:
                    self.server.clients[user_id]['message_handler'] = message_handler
                    logger.info(f"Message handler registered for user {user_id}")

            # Get recent messages
            recent_messages = self.server.get_room_messages(room_id)

            for msg_data in recent_messages:
                # Convert to ChatMessage
                msg = chat_pb2.ChatMessage(
                    user_id=msg_data['user_id'],
                    username=msg_data['username'],
                    content=msg_data['content'],
                    room_id=msg_data['room_id'],
                    timestamp=msg_data['timestamp'],
                    message_id=msg_data['message_id']
                )

                yield msg

            # Send server status
            status = chat_pb2.ServerStatus(
                server_id=self.server.server_id,
                role=self.server.raft.state.name,
                leader_id=self.server.raft.leader_id or "",
                term=self.server.raft.current_term,
                peer_servers=list(self.server.peer_addresses.keys())
            )

            yield status

            # Send join message to all users

            join_message = chat_pb2.ChatMessage(
                user_id="system",
                username="System",
                content=f"{username} has joined the room",
                room_id=room_id,
                timestamp=int(time.time()),
                message_id=str(uuid.uuid4())
            )
            self.server.send_message(join_message)

            # Start main loop to process messages and keep connection
            try:
                while context.is_active():
                    # Wait for new messages or timeout
                    with message_condition:
                        if not message_queue:
                            # Wait for new messages, maximum 1 second
                            message_condition.wait(1)

                        # Print debug info
                        if message_queue:
                            logger.info(f"Processing {len(message_queue)} messages for user {user_id}")

                        # Process all messages in the queue
                        msgs = message_queue.copy()
                        message_queue.clear()

                    # Send messages
                    for msg in msgs:
                        logger.info(f"Yielding message to client {user_id}: {msg.username}: {msg.content[:30]}...")
                        yield msg

            except Exception as e:
                logger.error(f"Error in JoinChat: {e}")

            finally:
                # User leaves room
                self.server.leave_room(user_id)

        def SendMessage(self, request, context):
            """Handle sending a chat message."""
            # Check if we're the leader
            if self.server.raft.state != ServerState.LEADER:
                # Redirect to leader
                if self.server.raft.leader_id:
                    leader_info = self.server.peer_addresses.get(
                        self.server.raft.leader_id
                    )

                    if leader_info:
                        host, port = leader_info
                        return chat_pb2.SendResponse(
                            success=False,
                            error=f"Not the leader. Redirect to {host}:{port}"
                        )

                return chat_pb2.SendResponse(
                    success=False,
                    error="Not the leader. No leader known."
                )

            # Process message
            message_id = request.message_id or str(uuid.uuid4())

            # Update message ID if needed
            if not request.message_id:
                request.message_id = message_id

            # Set timestamp if not provided
            if not request.timestamp:
                request.timestamp = int(time.time())

            # Send the message
            success = self.server.send_message(request)

            # Return response
            return chat_pb2.SendResponse(
                success=success,
                message_id=message_id
            )

        def GetServerStatus(self, request, context):
            """Get server status."""
            status = self.server.get_server_status()

            return chat_pb2.ServerStatus(
                server_id=status['server_id'],
                role=status['raft_status']['state'],
                leader_id=status['raft_status']['leader_id'] or "",
                term=status['raft_status']['current_term'],
                peer_servers=list(self.server.peer_addresses.keys())
            )


def main():
    """Main entry point for the chat server."""
    parser = argparse.ArgumentParser(description='Distributed Chat Server')

    parser.add_argument('--id', required=True, help='Server ID')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on')
    parser.add_argument('--dns', help='DNS service address (host:port)')
    parser.add_argument('--peers', help='Comma-separated list of peer addresses (id=host:port,...)')

    args = parser.parse_args()

    # Parse peer addresses
    peer_addresses = {}
    if args.peers:
        for peer in args.peers.split(','):
            if '=' in peer and ':' in peer:
                peer_id, addr = peer.split('=', 1)
                host, port = addr.split(':', 1)
                peer_addresses[peer_id] = (host, int(port))

    # Create and start server
    server = ChatServer(
        server_id=args.id,
        host=args.host,
        port=args.port,
        peer_addresses=peer_addresses,
        dns_address=args.dns
    )

    # Create default room
    server.create_room('default', 'General Chat', 'Default chat room')

    # Start server
    server.start()

    # Wait for termination
    server.wait_for_termination()


if __name__ == '__main__':
    main()