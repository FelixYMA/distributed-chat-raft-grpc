import threading
import json
import uuid
import logging
import time
import os
import grpc
from collections import defaultdict

# Import consistent hash
from consistent_hash import ConsistentHash

logger = logging.getLogger(__name__)


class DataStore:
    """
    Distributed data store with replication using consistent hashing.
    """
    
    def __init__(self, server_id, peer_addresses, replication_factor=3, data_dir='./data'):
        """
        Initialize the data store.
        
        Args:
            server_id: This server's identifier
            peer_addresses: Dictionary mapping server IDs to (host, port) tuples
            replication_factor: Number of replicas to maintain
            data_dir: Directory to store data
        """
        self.server_id = server_id
        self.peer_addresses = peer_addresses
        self.replication_factor = min(replication_factor, len(peer_addresses))
        self.data_dir = f"{data_dir}/{server_id}"
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # In-memory copy of data
        self.data = defaultdict(dict)  # Map of collection -> {key -> value}
        
        # Set up consistent hash ring
        self.hash_ring = ConsistentHash(nodes=list(peer_addresses.keys()))
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Load data from disk
        self._load_data_from_disk()
        
        # Set up gRPC stubs for peers
        self.peer_stubs = {}
        # We'll set these up later when we have the proper gRPC service
        
        logger.info(f"DataStore initialized for server {server_id} with replication factor {replication_factor}")
    
    def _load_data_from_disk(self):
        """Load all data from disk storage."""
        try:
            # Find all JSON files in the data directory
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.json'):
                    collection = filename.split('.')[0]
                    file_path = os.path.join(self.data_dir, filename)
                    
                    with open(file_path, 'r') as f:
                        self.data[collection] = json.load(f)
                    
                    logger.info(f"Loaded {len(self.data[collection])} items from {collection}")
        except Exception as e:
            logger.error(f"Error loading data from disk: {e}")
    
    def _save_collection_to_disk(self, collection):
        """Save a collection to disk."""
        try:
            file_path = os.path.join(self.data_dir, f"{collection}.json")
            
            with open(file_path, 'w') as f:
                json.dump(self.data[collection], f)
            
            logger.debug(f"Saved collection {collection} to disk")
        except Exception as e:
            logger.error(f"Error saving collection {collection} to disk: {e}")

    def get(self, collection, key, local_only=False):
        """
        Get an item from the data store.

        Args:
            collection: The collection name
            key: The item key
            local_only: If True, only check local storage

        Returns:
            The item value or None if not found
        """
        with self.lock:
            # Special handling of default rooms
            if collection == 'rooms' and key == 'default':
                # If there is no default room locally, create a
                if 'rooms' not in self.data or 'default' not in self.data['rooms']:
                    self.data.setdefault('rooms', {})['default'] = {
                        'room_id': 'default',
                        'name': 'General Chat',
                        'description': 'Default chat room',
                        'created_at': time.time(),
                        'created_by': self.server_id
                    }
                    self._save_collection_to_disk('rooms')
                    logger.info(f"Created missing default room locally")
                return self.data['rooms']['default']

            # Normal checking of local data
            if collection in self.data and key in self.data[collection]:
                return self.data[collection][key]

            if local_only:
                return None

            # Find the responsible node
            responsible_nodes = self.hash_ring.get_nodes(f"{collection}:{key}", self.replication_factor)

            # If we're in charge and we don't find it, it really doesn't exist.
            if self.server_id in responsible_nodes:
                if responsible_nodes[0] == self.server_id:
                    return None

            # Trying to get from other responsible nodes
            # The treatment is simplified here by assuming that there is no
            return None
    
    def put(self, collection, key, value, replicate=True):
        """
        Put an item in the data store.
        
        Args:
            collection: The collection name
            key: The item key
            value: The item value
            replicate: Whether to replicate to other nodes
            
        Returns:
            True if successful, False otherwise
        """
        with self.lock:
            # Determine responsible nodes
            responsible_nodes = self.hash_ring.get_nodes(f"{collection}:{key}", self.replication_factor)
            
            logger.info(f"Responsible nodes for {collection}:{key}: {responsible_nodes}")
            
            # If we're responsible, store locally
            if self.server_id in responsible_nodes:
                self.data[collection][key] = value
                self._save_collection_to_disk(collection)
                logger.info(f"Stored {collection}:{key} locally")
            
            # If we should replicate and we're the coordinator (first request receiver)
            if replicate:
                for node in responsible_nodes:
                    if node != self.server_id:
                        # This would use gRPC to replicate to other nodes
                        # For now, just log
                        logger.info(f"Would replicate {collection}:{key} to {node}")
            
            return True
    
    def delete(self, collection, key, replicate=True):
        """
        Delete an item from the data store.
        
        Args:
            collection: The collection name
            key: The item key
            replicate: Whether to replicate the deletion
            
        Returns:
            True if deleted, False if not found
        """
        with self.lock:
            # Determine responsible nodes
            responsible_nodes = self.hash_ring.get_nodes(f"{collection}:{key}", self.replication_factor)
            
            # Check if we're responsible
            if self.server_id in responsible_nodes:
                if collection in self.data and key in self.data[collection]:
                    del self.data[collection][key]
                    self._save_collection_to_disk(collection)
                    logger.info(f"Deleted {collection}:{key} locally")
                    
                    # Replicate to other responsible nodes
                    if replicate:
                        for node in responsible_nodes:
                            if node != self.server_id:
                                # Use gRPC to replicate deletion
                                # For now, just log
                                logger.info(f"Would delete {collection}:{key} from {node}")
                    
                    return True
            
            # If we're not responsible, forward to a responsible node
            if replicate and responsible_nodes:
                # This would use gRPC to forward the deletion request
                # For now, just log
                logger.info(f"Would forward delete of {collection}:{key} to {responsible_nodes[0]}")
            
            return False
    
    def get_all(self, collection, local_only=False):
        """
        Get all items in a collection.
        
        Args:
            collection: The collection name
            local_only: If True, only return local items
            
        Returns:
            Dictionary of items in the collection
        """
        with self.lock:
            result = {}
            
            # Add local items
            if collection in self.data:
                result.update(self.data[collection])
            
            if local_only:
                return result
            
            # In a real implementation, we would query all servers
            # For simplicity, just return local data
            return result
    
    def add_server(self, server_id, address, port):
        """
        Add a new server to the system.
        
        Args:
            server_id: The new server's identifier
            address: The server's address
            port: The server's port
            
        Returns:
            True if successful
        """
        with self.lock:
            if server_id in self.peer_addresses:
                logger.warning(f"Server {server_id} already exists")
                return False
            
            # Add to peer addresses
            self.peer_addresses[server_id] = (address, port)
            
            # Add to hash ring
            self.hash_ring.add_node(server_id)
            
            # Rebalance data
            self._rebalance_data()
            
            return True
    
    def remove_server(self, server_id):
        """
        Remove a server from the system.
        
        Args:
            server_id: The server's identifier
            
        Returns:
            True if successful
        """
        with self.lock:
            if server_id not in self.peer_addresses:
                logger.warning(f"Server {server_id} doesn't exist")
                return False
            
            # Remove from peer addresses
            del self.peer_addresses[server_id]
            
            # Remove from hash ring
            self.hash_ring.remove_node(server_id)
            
            # Rebalance data
            self._rebalance_data()
            
            return True
    
    def _rebalance_data(self):
        """Rebalance data after cluster membership changes."""
        with self.lock:
            # Go through all local data
            # For simplicity, we'll just log what would happen
            for collection in self.data:
                for key in list(self.data[collection].keys()):
                    # Determine responsible nodes for this key
                    responsible_nodes = self.hash_ring.get_nodes(
                        f"{collection}:{key}", 
                        self.replication_factor
                    )
                    
                    # If we're no longer responsible, this data should be moved
                    if self.server_id not in responsible_nodes:
                        value = self.data[collection][key]
                        logger.info(f"Would move {collection}:{key} to {responsible_nodes}")
                        
                        # In a real implementation, we would replicate to new nodes
                        # before removing locally
                    
                    # If we are responsible but not the primary, we might need to
                    # get data from other nodes
                    elif responsible_nodes[0] != self.server_id:
                        logger.info(f"No longer primary for {collection}:{key}")
    
    def store_chat_message(self, message):
        """
        Store a chat message using consistent hashing.
        
        Args:
            message: The chat message object
            
        Returns:
            The stored message with generated ID
        """
        # Generate a unique ID if not present
        if not hasattr(message, 'message_id') or not message.message_id:
            message.message_id = str(uuid.uuid4())
        
        # Serialize the message
        message_data = {
            'user_id': message.user_id,
            'username': message.username,
            'content': message.content,
            'room_id': message.room_id,
            'timestamp': message.timestamp,
            'message_id': message.message_id
        }
        
        # Store using the message ID as key
        self.put('chat_messages', message.message_id, message_data)
        
        # Also store by room for efficient retrieval
        room_messages = self.get_all(f'room_{message.room_id}_messages', local_only=True)
        room_messages[message.message_id] = message_data
        self.put(f'room_{message.room_id}_messages', 'messages', room_messages)
        
        return message
    
    def get_chat_messages(self, room_id, limit=100):
        """
        Get chat messages for a room.
        
        Args:
            room_id: The room ID
            limit: Maximum number of messages to return
            
        Returns:
            List of messages
        """
        # Get messages for the room
        room_messages = self.get(f'room_{room_id}_messages', 'messages')
        
        if not room_messages:
            return []
        
        # Sort by timestamp
        sorted_messages = sorted(
            room_messages.values(),
            key=lambda msg: msg.get('timestamp', 0),
            reverse=True
        )
        
        # Limit the number of messages
        return sorted_messages[:limit]
