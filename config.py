"""
Configuration settings for the distributed chat system.
"""

import os

# Server settings
SERVER_HOST = os.environ.get('SERVER_HOST', 'localhost')
SERVER_PORT_BASE = int(os.environ.get('SERVER_PORT_BASE', 50050))

# RAFT settings
RAFT_PORT_OFFSET = 100  # RAFT ports are SERVER_PORT + RAFT_PORT_OFFSET
RAFT_ELECTION_TIMEOUT_MIN = 150  # milliseconds
RAFT_ELECTION_TIMEOUT_MAX = 300  # milliseconds
RAFT_HEARTBEAT_INTERVAL = 50  # milliseconds

# Data store settings
DATA_DIR = os.environ.get('DATA_DIR', './data')
REPLICATION_FACTOR = 3  # Number of replicas for each piece of data

# Consistent hashing settings
VIRTUAL_NODES = 100  # Number of virtual nodes per physical node

# DNS settings
DNS_HOST = os.environ.get('DNS_HOST', 'localhost')
DNS_PORT = int(os.environ.get('DNS_PORT', 50051))
DNS_HEARTBEAT_INTERVAL = 5  # seconds
DNS_RECORD_TIMEOUT = 15  # seconds

# MapReduce settings
MAPREDUCE_WORKER_COUNT = 4  # Number of worker threads per node

# Client settings
CLIENT_RECONNECT_ATTEMPTS = 5
CLIENT_RECONNECT_DELAY = 2  # seconds

# Room settings
DEFAULT_ROOM_ID = 'default'
DEFAULT_ROOM_NAME = 'General Chat'
DEFAULT_ROOM_DESCRIPTION = 'Default chat room'

# Logging settings
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
