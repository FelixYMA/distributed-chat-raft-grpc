import hashlib
import bisect
import logging

logger = logging.getLogger(__name__)


class ConsistentHash:
    """Consistent hashing implementation for distributing data across nodes."""
    
    def __init__(self, nodes=None, replicas=100):
        """
        Initialize the consistent hash ring.
        
        Args:
            nodes: Initial list of node identifiers
            replicas: Number of virtual nodes per real node
        """
        self.replicas = replicas
        self.ring = {}  # Map of hash position to node
        self.sorted_keys = []  # Sorted list of hash positions
        
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        """Generate a hash value for a key."""
        key_bytes = str(key).encode('utf-8')
        return int(hashlib.md5(key_bytes).hexdigest(), 16)
    
    def add_node(self, node):
        """Add a node to the hash ring."""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            
        # Update sorted keys
        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Added node {node} to consistent hash ring")
    
    def remove_node(self, node):
        """Remove a node from the hash ring."""
        keys_to_remove = []
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            if key in self.ring:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.ring[key]
        
        # Update sorted keys
        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Removed node {node} from consistent hash ring")
    
    def get_node(self, key):
        """
        Get the node that should store the given key.
        
        Args:
            key: The key to locate
            
        Returns:
            The node responsible for the key
        """
        if not self.ring:
            return None
        
        hash_key = self._hash(key)
        
        # Find the first hash position >= hash_key
        idx = bisect.bisect_left(self.sorted_keys, hash_key) % len(self.sorted_keys)
        
        # Return the corresponding node
        return self.ring[self.sorted_keys[idx]]
    
    def get_nodes(self, key, n):
        """
        Get n nodes for storing replicas of the key.
        
        Args:
            key: The key to locate
            n: Number of nodes to return
            
        Returns:
            List of n nodes responsible for the key
        """
        if not self.ring:
            return []
        
        if n > len(set(self.ring.values())):
            n = len(set(self.ring.values()))
        
        hash_key = self._hash(key)
        
        # Find the first position >= hash_key
        idx = bisect.bisect_left(self.sorted_keys, hash_key) % len(self.sorted_keys)
        
        # Collect n unique nodes
        nodes = []
        unique_nodes = set()
        
        while len(unique_nodes) < n:
            node = self.ring[self.sorted_keys[idx]]
            if node not in unique_nodes:
                nodes.append(node)
                unique_nodes.add(node)
            
            idx = (idx + 1) % len(self.sorted_keys)
        
        return nodes
    
    def get_node_allocation(self):
        """
        Get the current node allocation statistics.
        
        Returns:
            A dictionary mapping each node to the number of keys it's responsible for
        """
        node_count = {}
        for node in set(self.ring.values()):
            node_count[node] = 0
        
        for key in self.sorted_keys:
            node = self.ring[key]
            node_count[node] += 1
        
        return node_count