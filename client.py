import logging
import threading
import time
import uuid
import grpc
import argparse
import sys
import os
import queue

# Import generated gRPC code (will be generated from chat.proto)
from proto import chat_pb2
from proto import chat_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_local_ip():
    """Obtain the IP address of this machine that can be used for external connections"""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No real connection required
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


class ChatClient:
    """
    Chat client implementation for connecting to the distributed chat system.
    """

    def __init__(self, server_address, dns_address=None):
        """
        Initialize the chat client.

        Args:
            server_address: Initial server address (host:port)
            dns_address: DNS service address for service discovery
        """
        self.server_address = server_address
        self.dns_address = dns_address

        # User information
        self.user_id = str(uuid.uuid4())
        self.username = None
        self.current_room = 'default'

        # Server information
        self.current_server = {'address': server_address}
        self.leader_server = None

        # Connection state
        self.channel = None
        self.stub = None
        self.message_stream = None
        self.connected = False

        # Message display queue
        self.message_queue = queue.Queue()

        # DNS client
        self.dns_client = None
        if dns_address:
            self._init_dns_client()

    def _init_dns_client(self):
        """Initialize the DNS client for service discovery."""
        try:
            channel = grpc.insecure_channel(self.dns_address)
            self.dns_client = chat_pb2_grpc.DNSServiceStub(channel)
            logger.info(f"DNS client initialized with {self.dns_address}")
        except Exception as e:
            logger.error(f"Error initializing DNS client: {e}")

    def _find_leader_server(self):
        """
        Find the leader server using DNS.

        Returns:
            Address of the leader server or None if not found
        """
        if not self.dns_client:
            return None

        try:
            # Look up the leader server
            response = self.dns_client.Lookup(chat_pb2.LookupRequest(
                server_name="leader"
            ))

            if response:
                return f"{response.address}:{response.port}"

            # If no dedicated leader entry, find server with is_leader=True
            # Get all servers
            servers = {}
            # This would be a real DNS query in a full implementation

            for name, info in servers.items():
                if info['is_leader']:
                    return f"{info['address']}:{info['port']}"

        except Exception as e:
            logger.error(f"Error finding leader server: {e}")

        return None

    def connect(self):
        """
        Connect to the chat server.

        Returns:
            True if connected successfully
        """
        if self.connected:
            self.disconnect()

        # Try DNS first if available
        leader_address = self._find_leader_server()
        if leader_address:
            server_address = leader_address
            logger.info(f"Using leader server from DNS: {server_address}")
        else:
            server_address = self.server_address

        try:
            # Create channel and stub
            self.channel = grpc.insecure_channel(server_address)
            self.stub = chat_pb2_grpc.ChatServiceStub(self.channel)

            # Try to detect if the server is responding to Ping
            try:
                response = self.stub.Ping(chat_pb2.PingRequest())
                if response.success:
                    self.connected = True
                    self.current_server['address'] = server_address
                    logger.info(f"Connected to server at {server_address}")
                    return True
                else:
                    logger.error(f"Ping failed: {response.message}")
                    return False
            except Exception as e:
                logger.error(f"Ping exception: {e}")
                return False

        except Exception as e:
            logger.error(f"Error connecting to server: {e}")
            return False

    def disconnect(self):
        """Disconnect from the server."""
        if self.message_stream:
            # Close stream
            self.message_stream = None

        if self.channel:
            # Close channel
            self.channel.close()
            self.channel = None

        self.stub = None
        self.connected = False
        logger.info("Disconnected from server")

    def login(self, username):
        """
        Login with a username.

        Args:
            username: Display name to use

        Returns:
            True if logged in successfully
        """
        self.username = username
        logger.info(f"Logged in as {username} (User ID: {self.user_id})")
        return True

    def join_chat(self, room_id='default'):
        """
        Join a chat room.

        Args:
            room_id: Room to join

        Returns:
            True if joined successfully
        """
        if not self.connected or not self.username:
            logger.error("Not connected or not logged in")
            return False

        try:
            # Create join request
            request = chat_pb2.JoinRequest(
                user_id=self.user_id,
                username=self.username
            )

            # Start message stream
            self.message_stream = self.stub.JoinChat(request)
            self.current_room = room_id

            # Start message handler thread
            threading.Thread(
                target=self._message_handler,
                daemon=True
            ).start()

            # Start display thread if not already running
            if not hasattr(self, 'display_thread') or not self.display_thread.is_alive():
                self.display_thread = threading.Thread(
                    target=self._display_messages,
                    daemon=True
                )
                self.display_thread.start()

            logger.info(f"Joined room {room_id}")
            return True

        except grpc.RpcError as e:
            status_code = e.code()
            details = e.details()

            logger.error(f"Error joining chat: {status_code} - {details}")

            # Check for leader redirect
            if status_code == grpc.StatusCode.FAILED_PRECONDITION and "Redirect to" in details:
                # Extract new leader address
                leader_address = details.split("Redirect to ")[1]

                logger.info(f"Redirecting to leader: {leader_address}")

                # Update server address
                self.server_address = leader_address

                # Reconnect and try again
                if self.connect():
                    return self.join_chat(room_id)

            return False

        except Exception as e:
            logger.error(f"Error joining chat: {e}")
            return False

    def send_message(self, content):
        """
        Send a chat message.

        Args:
            content: Message content

        Returns:
            True if sent successfully
        """
        if not self.connected or not self.username:
            logger.error("Not connected or not logged in")
            return False

        try:
            # Create message
            message = chat_pb2.ChatMessage(
                user_id=self.user_id,
                username=self.username,
                content=content,
                room_id=self.current_room,
                timestamp=int(time.time())
            )

            # Send message
            response = self.stub.SendMessage(message)

            if not response.success:
                logger.error(f"Failed to send message: {response.error}")

                # Check for leader redirect
                if "Redirect to" in response.error:
                    # Extract new leader address
                    leader_address = response.error.split("Redirect to ")[1]

                    logger.info(f"Redirecting to leader: {leader_address}")

                    # Update server address
                    self.server_address = leader_address

                    # Reconnect and try again
                    if self.connect():
                        return self.send_message(content)

                return False

            return True

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def _message_handler(self):
        """Handle incoming messages from the server."""
        try:
            logger.info("Starting message handler thread")
            for message in self.message_stream:
                # Check message type
                if message.DESCRIPTOR.name == 'ChatMessage':
                    # Add to message queue for display
                    message_data = {
                        'type': 'chat',
                        'username': message.username,
                        'content': message.content,
                        'timestamp': message.timestamp
                    }
                    self.message_queue.put(message_data)

                    # Add a direct callback to display the message immediately
                    if hasattr(self, 'display_callback') and self.display_callback:
                        try:
                            self.display_callback(message_data)
                        except Exception as e:
                            print(f"执行显示回调时出错: {e}")

                    # Add debug output
                    print(f"CLIENT received message and added to queue: {message.username}: {message.content}")
                    logger.info(f"Received message: {message.username}: {message.content}")

                elif message.DESCRIPTOR.name == 'ServerStatus':
                    # Update server info
                    self.current_server['id'] = message.server_id
                    self.current_server['role'] = message.role
                    self.current_server['leader_id'] = message.leader_id

                    # Add to message queue for display
                    message_data = {
                        'type': 'status',
                        'server_id': message.server_id,
                        'role': message.role,
                        'leader_id': message.leader_id
                    }
                    self.message_queue.put(message_data)

                    # Add a status update callback
                    if hasattr(self, 'status_callback') and self.status_callback:
                        try:
                            self.status_callback(message_data)
                        except Exception as e:
                            print(f"执行状态回调时出错: {e}")

                    # Add debug output
                    print(f"CLIENT received status update: {message.server_id} ({message.role})")
                    logger.info(f"Received status update: {message.server_id} ({message.role})")

        except grpc.RpcError as e:
            logger.error(f"Stream error: {e}")

            # Connection lost, try to reconnect
            self.connected = False
            self.message_stream = None
            threading.Thread(target=self._reconnect_thread, daemon=True).start()

        except Exception as e:
            logger.error(f"Message handler error: {e}")
            self.connected = False

    def _reconnect_thread(self):
        """Attempts to reconnect in a separate thread to avoid blocking the main thread"""
        try:
            # Waiting a while for the election of a possible new LEADER to be completed
            time.sleep(3)

            # Trying to reconnect
            if self.reconnect():
                # If the reconnection is successful, display the message
                self.message_queue.put({
                    'type': 'chat',
                    'username': 'System',
                    'content': 'Reconnected to server',
                    'timestamp': int(time.time())
                })
            else:
                # Failed to reconnect, display message
                self.message_queue.put({
                    'type': 'chat',
                    'username': 'System',
                    'content': 'Failed to reconnect to any server',
                    'timestamp': int(time.time())
                })
        except Exception as e:
            logger.error(f"Error in reconnection thread: {e}")

    def _display_messages(self):
        """Display messages from the queue."""
        while True:
            try:
                # Get message from queue
                message = self.message_queue.get()

                if message['type'] == 'chat':
                    # Format timestamp
                    timestamp = time.strftime('%H:%M:%S', time.localtime(message['timestamp']))

                    # Print chat message
                    print(f"\r[{timestamp}] {message['username']}: {message['content']}")

                elif message['type'] == 'status':
                    # Print server status
                    print(f"\r[SERVER] {message['server_id']} ({message['role']})")

                    if message['leader_id']:
                        print(f"\r[SERVER] Leader: {message['leader_id']}")

                # Done with this message
                self.message_queue.task_done()

                # Prompt for next message
                sys.stdout.write("> ")
                sys.stdout.flush()

            except Exception as e:
                logger.error(f"Display error: {e}")

    def reconnect(self):
        """
        Attempt to reconnect to the server with improved leader discovery.

        Returns:
            True if reconnected successfully
        """
        max_attempts = 10
        attempt = 0

        # Save current room and username for rejoining after reconnecting
        current_room = self.current_room
        current_username = self.username

        while attempt < max_attempts:
            attempt += 1
            logger.info(f"Reconnect attempt {attempt}/{max_attempts}")

            # Try a DNS lookup first
            leader_address = self._find_leader_server()
            if leader_address:
                self.server_address = leader_address
                logger.info(f"Found leader via DNS: {leader_address}")

            # If the DNS is not found, try polling another server
            if not leader_address:
                base_addr = self.server_address.split(':')[0]
                # Try other common ports
                for port in [50050, 50060, 50070, 50080, 50090]:
                    try:
                        test_address = f"{base_addr}:{port}"
                        # Avoid trying addresses that have failed
                        if test_address == self.server_address:
                            continue

                        logger.info(f"Trying alternate server: {test_address}")
                        self.server_address = test_address
                        if self.connect():
                            # If the connection is successful, check to see if it is the LEADER
                            status = self.get_server_status()
                            if status and status.get('role') == 'LEADER':
                                logger.info(f"Connected to leader at {test_address}")
                                # Logging back in and joining chat rooms
                                if self.username:
                                    self.login(self.username)
                                    self.join_chat(self.current_room)
                                    return True
                    except Exception as e:
                        logger.warning(f"Failed to connect to {test_address}: {e}")
                        continue

            # Regular Connection Attempts
            if self.connect():
                # Logging back in and joining chat rooms
                if self.username:
                    self.login(self.username)
                    if self.join_chat(self.current_room):
                        return True

            # Waiting for the next attempt
            time.sleep(1)

        logger.error("Failed to reconnect after multiple attempts")
        return False

    def get_server_status(self):
        """
        Get current server status.

        Returns:
            Server status information
        """
        if not self.connected:
            logger.error("Not connected")
            return None

        try:
            # Request server status
            request = chat_pb2.StatusRequest(
                user_id=self.user_id
            )

            response = self.stub.GetServerStatus(request)

            return {
                'server_id': response.server_id,
                'role': response.role,
                'leader_id': response.leader_id,
                'term': response.term,
                'peer_servers': list(response.peer_servers)
            }

        except Exception as e:
            logger.error(f"Error getting server status: {e}")
            return None

    def run_interactive(self):
        """Run an interactive chat session."""
        print("=== Distributed Chat System Client ===")

        # Get username
        username = input("Enter your username: ")

        # Login
        if not self.login(username):
            print("Login failed")
            return

        # Connect to server
        if not self.connect():
            print("Connection failed")
            return

        # Join default room
        if not self.join_chat('default'):
            print("Failed to join chat")
            return

        print(f"Connected as {username}")
        print("Type your messages (or /quit to exit, /status for server info)")

        try:
            while True:
                # Get user input
                message = input("> ")

                if message.lower() == '/quit':
                    break

                elif message.lower() == '/status':
                    # Get and display server status
                    status = self.get_server_status()
                    if status:
                        print(f"Server: {status['server_id']} ({status['role']})")
                        print(f"Leader: {status['leader_id']}")
                        print(f"Term: {status['term']}")
                        print(f"Peers: {', '.join(status['peer_servers'])}")

                elif message.strip():
                    # Send message
                    self.send_message(message)

        except KeyboardInterrupt:
            pass

        finally:
            # Disconnect
            self.disconnect()
            print("Disconnected")


def main():
    """Main entry point for the chat client."""
    parser = argparse.ArgumentParser(description='Distributed Chat Client')

    parser.add_argument('--server', required=True, help='Server address (host:port)')
    parser.add_argument('--dns', help='DNS service address (host:port)')

    args = parser.parse_args()

    # 创建并运行客户端
    client = ChatClient(
        server_address=args.server,
        dns_address=args.dns
    )

    client.run_interactive()


if __name__ == '__main__':
    main()