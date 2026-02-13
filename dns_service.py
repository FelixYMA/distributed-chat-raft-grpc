import threading
import logging
import time
import grpc
from concurrent import futures

# Import generated gRPC code (will be generated from chat.proto)
# For now we'll use placeholder imports
from proto import chat_pb2
from proto import chat_pb2_grpc

logger = logging.getLogger(__name__)

class DNSService:
    """
    Simple DNS service for service discovery within the distributed chat system.
    """
    
    def __init__(self, port=50051):
        """
        Initialize the DNS service.
        
        Args:
            port: Port to listen on
        """
        self.port = port
        
        # DNS records
        # {name: {address, port, is_leader, last_heartbeat}}
        self.records = {}
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Start the record cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_stale_records)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()
        
        logger.info(f"DNS service initialized on port {port}")
    
    def register(self, server_name, address, port, is_leader=False):
        """
        Register a server with the DNS service.
        
        Args:
            server_name: Name of the server
            address: Server address
            port: Server port
            is_leader: Whether this server is the leader
            
        Returns:
            True if successful
        """
        with self.lock:
            self.records[server_name] = {
                'address': address,
                'port': port,
                'is_leader': is_leader,
                'last_heartbeat': time.time()
            }
            
            logger.info(f"Registered server {server_name} at {address}:{port}")
            
            # If this is a leader, update other records
            if is_leader:
                for name, record in self.records.items():
                    if name != server_name:
                        record['is_leader'] = False
            
            return True
    
    def lookup(self, server_name):
        """
        Look up a server by name.
        
        Args:
            server_name: Name of the server to look up
            
        Returns:
            Dictionary with server details or None if not found
        """
        with self.lock:
            if server_name in self.records:
                record = self.records[server_name]
                
                # Update last access time
                record['last_accessed'] = time.time()
                
                return {
                    'address': record['address'],
                    'port': record['port'],
                    'is_leader': record['is_leader']
                }
            
            return None
    
    def find_leader(self):
        """
        Find the current leader server.
        
        Returns:
            Dictionary with leader details or None if not found
        """
        with self.lock:
            for name, record in self.records.items():
                if record['is_leader']:
                    return {
                        'name': name,
                        'address': record['address'],
                        'port': record['port']
                    }
            
            return None
    
    def heartbeat(self, server_name):
        """
        Update heartbeat for a server.
        
        Args:
            server_name: Name of the server
            
        Returns:
            True if successful, False if server not found
        """
        with self.lock:
            if server_name in self.records:
                self.records[server_name]['last_heartbeat'] = time.time()
                return True
            
            return False
    
    def _cleanup_stale_records(self):
        """Clean up stale DNS records periodically."""
        while True:
            time.sleep(5)  # Check every 5 seconds
            
            with self.lock:
                current_time = time.time()
                stale_threshold = 15  # 15 seconds without heartbeat
                
                stale_servers = []
                
                for name, record in self.records.items():
                    if current_time - record['last_heartbeat'] > stale_threshold:
                        stale_servers.append(name)
                
                for name in stale_servers:
                    logger.info(f"Removing stale server {name}")
                    del self.records[name]
    
    def get_all_servers(self):
        """
        Get all registered servers.
        
        Returns:
            Dictionary mapping server names to details
        """
        with self.lock:
            result = {}
            
            for name, record in self.records.items():
                result[name] = {
                    'address': record['address'],
                    'port': record['port'],
                    'is_leader': record['is_leader']
                }
            
            return result
    
    class DNSServicer(chat_pb2_grpc.DNSServiceServicer):
        """gRPC servicer for DNS service."""
        
        def __init__(self, dns_service):
            self.dns_service = dns_service
        
        def Lookup(self, request, context):
            """Handle DNS lookup requests."""
            server_name = request.server_name
            
            record = self.dns_service.lookup(server_name)
            
            if record:
                return chat_pb2.LookupResponse(
                    address=record['address'],
                    port=record['port'],
                    is_leader=record['is_leader']
                )
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Server {server_name} not found")
                return chat_pb2.LookupResponse()
        
        def Register(self, request, context):
            """Handle DNS registration requests."""
            server_name = request.server_name
            address = request.address
            port = request.port
            is_leader = request.is_leader
            
            success = self.dns_service.register(server_name, address, port, is_leader)
            
            return chat_pb2.RegisterResponse(
                success=success,
                error="" if success else "Failed to register server"
            )

    def start_server(self, host='0.0.0.0'):
        """Starting the DNS gRPC server"""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chat_pb2_grpc.add_DNSServiceServicer_to_server(
            self.DNSServicer(self), server
        )
        server.add_insecure_port(f"{host}:{self.port}")
        server.start()
        logger.info(f"DNS service started on {host}:{self.port}")
        return server


def main():
    """Main entry point for the DNS service."""
    import argparse
    parser = argparse.ArgumentParser(description='DNS Service')
    parser.add_argument('--port', type=int, default=50051, help='Port to listen on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind on')  # Add host parameter
    args = parser.parse_args()

    # Create and start DNS service
    dns_service = DNSService(port=args.port)
    server = dns_service.start_server(host=args.host)  # Pass in the host parameter

    try:
        print(f"DNS service started on port {args.port}")
        # Keep the server running
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("DNS service stopped")


if __name__ == "__main__":
    main()