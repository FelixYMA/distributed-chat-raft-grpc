import random
import time
import threading
import logging
import grpc
from enum import Enum
from concurrent import futures

# Import generated gRPC code (will be generated from chat.proto)
# For now we'll use placeholder imports
from proto import chat_pb2
from proto import chat_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServerState(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class RaftNode:
    def __init__(self, server_id, peer_addresses, port):
        self.server_id = server_id
        self.peer_addresses = peer_addresses  # List of (address, port) tuples
        self.port = port
        
        # RAFT state
        self.state = ServerState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry objects
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state (reinitialized after election)
        self.next_index = {}  # Map of server_id to next index
        self.match_index = {}  # Map of server_id to match index
        
        # Election timer
        self.election_timeout = self._get_random_election_timeout()
        self.last_heartbeat = time.time()
        
        # Lock for state updates
        self.lock = threading.RLock()
        
        # Leader ID (if known)
        self.leader_id = None
        
        # Callback for leader changes
        self.on_leader_change = None
        
        # Start election timer
        self.election_timer_thread = threading.Thread(target=self._run_election_timer)
        self.election_timer_thread.daemon = True
        self.election_timer_thread.start()
        
        # Start applying committed entries
        self.apply_thread = threading.Thread(target=self._apply_committed_entries)
        self.apply_thread.daemon = True
        self.apply_thread.start()
        
        # Set up gRPC stubs for peers
        self.peer_stubs = {}
        for peer_id, (peer_addr, peer_port) in peer_addresses.items():
            if peer_id != self.server_id:
                # Use the RAFT port (master port +100) instead of the server master port
                raft_port = peer_port + 100
                channel = grpc.insecure_channel(f"{peer_addr}:{raft_port}")
                self.peer_stubs[peer_id] = chat_pb2_grpc.RaftServiceStub(channel)
    
    def _get_random_election_timeout(self):
        # Random timeout between 150ms and 300ms
        return random.uniform(0.15, 0.3)
    
    def _run_election_timer(self):
        """Background thread for election timeout."""
        while True:
            time.sleep(0.05)  # Check every 50ms
            
            with self.lock:
                # Skip if we're the leader (leaders don't start elections)
                if self.state == ServerState.LEADER:
                    continue
                
                # Check if election timeout has elapsed
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self._start_election()
                    self.last_heartbeat = time.time()
                    self.election_timeout = self._get_random_election_timeout()
    
    def _start_election(self):
        """Start a leader election."""
        with self.lock:
            self.state = ServerState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.server_id
            votes_received = 1  # Vote for self
            
            logger.info(f"Node {self.server_id} starting election for term {self.current_term}")
            
            # Prepare RequestVote RPCs
            request = chat_pb2.VoteRequest(
                term=self.current_term,
                candidate_id=self.server_id,
                last_log_index=len(self.log) - 1,
                last_log_term=self.log[-1].term if self.log else 0
            )
            
            # Send RequestVote RPCs to all peers
            for peer_id, stub in self.peer_stubs.items():
                try:
                    response = stub.RequestVote(request, timeout=0.1)
                    
                    with self.lock:
                        # If response term is higher, revert to follower
                        if response.term > self.current_term:
                            self._become_follower(response.term)
                            return
                        
                        # Count votes
                        if response.vote_granted:
                            votes_received += 1
                except grpc.RpcError:
                    logger.warning(f"Failed to get vote from {peer_id}")
            
            # Check if we won the election
            if votes_received > (len(self.peer_addresses) + 1) / 2:
                self._become_leader()
    
    def _become_follower(self, term):
        """Transition to follower state."""
        logger.info(f"Node {self.server_id} becoming follower for term {term}")
        
        with self.lock:
            # Update state
            self.state = ServerState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            
            # Reset election timeout
            self.last_heartbeat = time.time()
            self.election_timeout = self._get_random_election_timeout()
    
    def _become_leader(self):
        """Transition to leader state."""
        logger.info(f"Node {self.server_id} becoming leader for term {self.current_term}")
        
        with self.lock:
            self.state = ServerState.LEADER
            self.leader_id = self.server_id
            
            # Initialize leader state
            for peer_id in self.peer_addresses:
                if peer_id != self.server_id:
                    self.next_index[peer_id] = len(self.log)
                    self.match_index[peer_id] = 0
            
            # Notify callback about leadership change
            if self.on_leader_change:
                self.on_leader_change(self.server_id)
            
            # Start sending heartbeats
            threading.Thread(target=self._send_heartbeats).start()
    
    def _send_heartbeats(self):
        """Send heartbeats to all followers."""
        while True:
            with self.lock:
                if self.state != ServerState.LEADER:
                    return
                
                for peer_id, stub in self.peer_stubs.items():
                    # Prepare AppendEntries RPC (heartbeat)
                    prev_log_index = self.next_index[peer_id] - 1
                    prev_log_term = 0
                    if prev_log_index >= 0 and prev_log_index < len(self.log):
                        prev_log_term = self.log[prev_log_index].term
                    
                    entries = []
                    # Add any new log entries
                    if self.next_index[peer_id] < len(self.log):
                        entries = self.log[self.next_index[peer_id]:]
                    
                    request = chat_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leader_id=self.server_id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit=self.commit_index
                    )
                    
                    try:
                        response = stub.AppendEntries(request, timeout=0.1)
                        
                        with self.lock:
                            # If response term is higher, revert to follower
                            if response.term > self.current_term:
                                self._become_follower(response.term)
                                return
                            
                            # Update nextIndex and matchIndex if successful
                            if response.success:
                                if entries:
                                    self.match_index[peer_id] = prev_log_index + len(entries)
                                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                            else:
                                # Decrement nextIndex and retry
                                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
                    except grpc.RpcError:
                        logger.warning(f"Failed to send heartbeat to {peer_id}")
            
            # Send heartbeats periodically
            time.sleep(0.05)  # 50ms
    
    def _apply_committed_entries(self):
        """Apply committed log entries to state machine."""
        while True:
            time.sleep(0.05)  # Check every 50ms
            
            with self.lock:
                if self.commit_index > self.last_applied:
                    for i in range(self.last_applied + 1, self.commit_index + 1):
                        # Apply log entry at index i
                        if i < len(self.log):
                            entry = self.log[i]
                            self._apply_log_entry(entry)
                    
                    self.last_applied = self.commit_index
    
    def _apply_log_entry(self, entry):
        """Apply a log entry to the state machine."""
        # This will be implemented in the chat server
        pass
    
    def _update_commit_index(self):
        """Update commit index based on match indices of followers."""
        if self.state != ServerState.LEADER:
            return
        
        # Find the largest N such that a majority of matchIndex[i] ≥ N
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue
            
            # Count servers with matchIndex >= n
            count = 1  # Include self
            for peer_id, match_idx in self.match_index.items():
                if match_idx >= n:
                    count += 1
            
            if count > (len(self.peer_addresses) + 1) / 2:
                self.commit_index = n
                return
    
    def append_entry(self, data):
        """Append a new entry to the log (only if leader)."""
        with self.lock:
            if self.state != ServerState.LEADER:
                return False
            
            # Create new log entry
            entry = chat_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log),
                data=data
            )
            
            # Append to log
            self.log.append(entry)
            
            return True
    
    def get_status(self):
        """Get the current status of this RAFT node."""
        with self.lock:
            return {
                "server_id": self.server_id,
                "state": self.state.name,
                "current_term": self.current_term,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "leader_id": self.leader_id
            }
    
    class RaftServicer(chat_pb2_grpc.RaftServiceServicer):
        """gRPC servicer for RAFT RPC calls."""
        
        def __init__(self, raft_node):
            self.raft_node = raft_node
        
        def RequestVote(self, request, context):
            """Handle RequestVote RPC."""
            with self.raft_node.lock:
                # If term is smaller than current term, reject
                if request.term < self.raft_node.current_term:
                    return chat_pb2.VoteResponse(
                        term=self.raft_node.current_term,
                        vote_granted=False
                    )
                
                # If term is larger, become follower
                if request.term > self.raft_node.current_term:
                    self.raft_node._become_follower(request.term)
                
                # Determine if log is up-to-date
                last_log_index = len(self.raft_node.log) - 1
                last_log_term = self.raft_node.log[last_log_index].term if self.raft_node.log else 0
                
                log_up_to_date = (
                    request.last_log_term > last_log_term or
                    (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
                )
                
                # Grant vote if we haven't voted yet and log is up-to-date
                vote_granted = (
                    (self.raft_node.voted_for is None or self.raft_node.voted_for == request.candidate_id) and
                    log_up_to_date
                )
                
                if vote_granted:
                    self.raft_node.voted_for = request.candidate_id
                    self.raft_node.last_heartbeat = time.time()  # Reset election timeout
                
                return chat_pb2.VoteResponse(
                    term=self.raft_node.current_term,
                    vote_granted=vote_granted
                )
        
        def AppendEntries(self, request, context):
            """Handle AppendEntries RPC (also used as heartbeat)."""
            with self.raft_node.lock:
                # If term is smaller than current term, reject
                if request.term < self.raft_node.current_term:
                    return chat_pb2.AppendEntriesResponse(
                        term=self.raft_node.current_term,
                        success=False,
                        match_index=0
                    )
                
                # Valid leader, reset election timeout
                self.raft_node.last_heartbeat = time.time()
                
                # If term is larger, become follower
                if request.term > self.raft_node.current_term:
                    self.raft_node._become_follower(request.term)
                
                # If we're a candidate, step down
                if self.raft_node.state == ServerState.CANDIDATE and request.term >= self.raft_node.current_term:
                    self.raft_node._become_follower(request.term)
                
                # Update leader ID
                self.raft_node.leader_id = request.leader_id
                
                # Check if log contains an entry at prev_log_index with matching term
                prev_log_index = request.prev_log_index
                if prev_log_index >= len(self.raft_node.log):
                    return chat_pb2.AppendEntriesResponse(
                        term=self.raft_node.current_term,
                        success=False,
                        match_index=len(self.raft_node.log) - 1
                    )
                
                if prev_log_index >= 0 and (
                    prev_log_index >= len(self.raft_node.log) or
                    self.raft_node.log[prev_log_index].term != request.prev_log_term
                ):
                    return chat_pb2.AppendEntriesResponse(
                        term=self.raft_node.current_term,
                        success=False,
                        match_index=prev_log_index - 1
                    )
                
                # If existing entries conflict with new entries, delete them
                # and all that follow
                if request.entries:
                    new_index = prev_log_index + 1
                    for i, entry in enumerate(request.entries):
                        if new_index + i < len(self.raft_node.log):
                            if self.raft_node.log[new_index + i].term != entry.term:
                                # Delete from this point
                                self.raft_node.log = self.raft_node.log[:new_index + i]
                                break
                    
                    # Append any new entries not already in the log
                    new_entries_start = max(0, len(self.raft_node.log) - (prev_log_index + 1))
                    self.raft_node.log.extend(request.entries[new_entries_start:])
                
                # Update commit index
                if request.leader_commit > self.raft_node.commit_index:
                    self.raft_node.commit_index = min(
                        request.leader_commit,
                        len(self.raft_node.log) - 1
                    )
                
                return chat_pb2.AppendEntriesResponse(
                    term=self.raft_node.current_term,
                    success=True,
                    match_index=prev_log_index + len(request.entries)
                )
        
        def RedirectToLeader(self, request, context):
            """Redirect client to the current leader."""
            with self.raft_node.lock:
                if self.raft_node.state == ServerState.LEADER:
                    # We are the leader
                    return chat_pb2.RedirectResponse(
                        leader_address="localhost",  # Change to actual address
                        leader_port=self.raft_node.port
                    )
                elif self.raft_node.leader_id:
                    # We know the leader
                    leader_addr, leader_port = self.raft_node.peer_addresses.get(
                        self.raft_node.leader_id, ("unknown", 0)
                    )
                    return chat_pb2.RedirectResponse(
                        leader_address=leader_addr,
                        leader_port=leader_port
                    )
                else:
                    # Leader unknown
                    return chat_pb2.RedirectResponse(
                        leader_address="",
                        leader_port=0
                    )
    
    def start_server(self):
        """Start the RAFT gRPC server."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chat_pb2_grpc.add_RaftServiceServicer_to_server(
            self.RaftServicer(self), server
        )
        server.add_insecure_port(f"[::]:{self.port}")
        server.start()
        logger.info(f"RAFT node {self.server_id} started on port {self.port}")
        return server
