"""
Clustered Key-Value Store with Replication
Implements primary-secondary replication with leader election
"""

import socket
import threading
import json
import os
import pickle
import time
import signal
import sys
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from enum import Enum
import random


class NodeRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    CANDIDATE = "candidate"


class WriteAheadLog:
    """Write-Ahead Log for ensuring durability"""
    
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.wal_file = None
        self._lock = threading.Lock()
        self._open_wal()
    
    def _open_wal(self):
        """Open WAL file in append mode"""
        self.wal_file = open(self.wal_path, 'ab', buffering=0)
    
    def log_operation(self, operation: Dict[str, Any]) -> None:
        """Log an operation to WAL with immediate sync"""
        with self._lock:
            entry = pickle.dumps(operation)
            length = len(entry).to_bytes(4, byteorder='big')
            self.wal_file.write(length + entry)
            self.wal_file.flush()
            os.fsync(self.wal_file.fileno())
    
    def replay(self) -> List[Dict[str, Any]]:
        """Replay operations from WAL"""
        operations = []
        if not os.path.exists(self.wal_path) or os.path.getsize(self.wal_path) == 0:
            return operations
        
        with open(self.wal_path, 'rb') as f:
            while True:
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                length = int.from_bytes(length_bytes, byteorder='big')
                entry_bytes = f.read(length)
                operation = pickle.loads(entry_bytes)
                operations.append(operation)
        
        return operations
    
    def checkpoint(self):
        """Truncate WAL after checkpoint"""
        with self._lock:
            self.wal_file.close()
            open(self.wal_path, 'wb').close()
            self._open_wal()
    
    def close(self):
        """Close WAL file"""
        if self.wal_file:
            self.wal_file.close()


class KVStore:
    """Main Key-Value Store with persistence"""
    
    def __init__(self, data_dir: str = "./kvstore_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.data_file = self.data_dir / "data.pkl"
        self.wal_file = self.data_dir / "wal.log"
        
        self.store: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        self.wal = WriteAheadLog(str(self.wal_file))
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load data from persistent storage and replay WAL"""
        if self.data_file.exists():
            with open(self.data_file, 'rb') as f:
                self.store = pickle.load(f)
            print(f"Loaded {len(self.store)} keys from checkpoint")
        
        operations = self.wal.replay()
        print(f"Replaying {len(operations)} operations from WAL")
        
        for op in operations:
            op_type = op['type']
            if op_type == 'set':
                self.store[op['key']] = op['value']
            elif op_type == 'delete':
                self.store.pop(op['key'], None)
            elif op_type == 'bulk_set':
                for key, value in op['items']:
                    self.store[key] = value
        
        print(f"Recovery complete. Total keys: {len(self.store)}")
    
    def set(self, key: str, value: Any, replicate: bool = True) -> bool:
        """Set a key-value pair"""
        with self._lock:
            self.wal.log_operation({'type': 'set', 'key': key, 'value': value})
            self.store[key] = value
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        with self._lock:
            return self.store.get(key)
    
    def delete(self, key: str, replicate: bool = True) -> bool:
        """Delete a key"""
        with self._lock:
            if key not in self.store:
                return False
            self.wal.log_operation({'type': 'delete', 'key': key})
            del self.store[key]
            return True
    
    def bulk_set(self, items: List[Tuple[str, Any]], replicate: bool = True) -> bool:
        """Bulk set operation (atomic)"""
        with self._lock:
            self.wal.log_operation({'type': 'bulk_set', 'items': items})
            for key, value in items:
                self.store[key] = value
            return True
    
    def get_all_data(self) -> Dict[str, Any]:
        """Get all data (for replication)"""
        with self._lock:
            return dict(self.store)
    
    def checkpoint(self):
        """Create a checkpoint and truncate WAL"""
        with self._lock:
            with open(self.data_file, 'wb') as f:
                pickle.dump(self.store, f)
            self.wal.checkpoint()
            print(f"Checkpoint created with {len(self.store)} keys")
    
    def shutdown(self):
        """Graceful shutdown"""
        print("Shutting down KVStore...")
        self.checkpoint()
        self.wal.close()
        print("Shutdown complete")


class ClusterNode:
    """Node in the cluster with replication support"""
    
    def __init__(self, node_id: int, host: str, port: int, 
                 peers: List[Tuple[str, int]], data_dir: str):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # List of (host, port) for peer nodes
        self.data_dir = data_dir
        
        self.kvstore = KVStore(data_dir)
        self.role = NodeRole.SECONDARY
        self.primary_node: Optional[Tuple[str, int]] = None
        
        self.server_socket = None
        self.running = False
        
        # Heartbeat and election
        self.last_heartbeat = time.time()
        self.heartbeat_timeout = 5.0  # seconds
        self.election_timeout = random.uniform(3.0, 6.0)
        self.term = 0
        self.voted_for: Optional[int] = None
        
        # Replication
        self.replication_connections: Dict[Tuple[str, int], socket.socket] = {}
        
        self._lock = threading.RLock()
    
    def start(self):
        """Start the cluster node"""
        self.running = True
        
        # Start client-facing server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print(f"Node {self.node_id} started on {self.host}:{self.port}")
        
        # Start heartbeat/election thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Start main server loop
        threading.Thread(target=self._server_loop, daemon=False).start()
    
    def _server_loop(self):
        """Main server loop"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address):
        """Handle client connection"""
        try:
            while True:
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    break
                
                message_length = int.from_bytes(length_bytes, byteorder='big')
                message_data = b''
                while len(message_data) < message_length:
                    chunk = client_socket.recv(min(message_length - len(message_data), 4096))
                    if not chunk:
                        break
                    message_data += chunk
                
                if len(message_data) != message_length:
                    break
                
                request = json.loads(message_data.decode('utf-8'))
                response = self._process_request(request)
                
                response_data = json.dumps(response).encode('utf-8')
                response_length = len(response_data).to_bytes(4, byteorder='big')
                client_socket.sendall(response_length + response_data)
                
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
    
    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process client request"""
        try:
            operation = request.get('operation')
            
            # Check if this is a cluster message
            if operation in ['HEARTBEAT', 'VOTE_REQUEST', 'VOTE_RESPONSE', 'REPLICATE']:
                return self._handle_cluster_message(request)
            
            # Only primary can handle writes
            if operation in ['SET', 'DELETE', 'BULK_SET']:
                if self.role != NodeRole.PRIMARY:
                    return {
                        'status': 'ERROR',
                        'message': 'Not primary',
                        'primary': self.primary_node
                    }
            
            # Process operation
            if operation == 'SET':
                key = request['key']
                value = request['value']
                success = self.kvstore.set(key, value)
                
                # Replicate to secondaries
                if self.role == NodeRole.PRIMARY:
                    self._replicate_operation(request)
                
                return {'status': 'OK', 'success': success}
            
            elif operation == 'GET':
                key = request['key']
                value = self.kvstore.get(key)
                if value is None:
                    return {'status': 'NOT_FOUND', 'value': None}
                return {'status': 'OK', 'value': value}
            
            elif operation == 'DELETE':
                key = request['key']
                success = self.kvstore.delete(key)
                
                if self.role == NodeRole.PRIMARY:
                    self._replicate_operation(request)
                
                return {'status': 'OK', 'success': success}
            
            elif operation == 'BULK_SET':
                items = [(item['key'], item['value']) for item in request['items']]
                success = self.kvstore.bulk_set(items)
                
                if self.role == NodeRole.PRIMARY:
                    self._replicate_operation(request)
                
                return {'status': 'OK', 'success': success}
            
            else:
                return {'status': 'ERROR', 'message': 'Unknown operation'}
        
        except Exception as e:
            return {'status': 'ERROR', 'message': str(e)}
    
    def _handle_cluster_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle cluster communication messages"""
        msg_type = message.get('operation')
        
        if msg_type == 'HEARTBEAT':
            # Received heartbeat from primary
            self.last_heartbeat = time.time()
            self.primary_node = (message.get('primary_host'), message.get('primary_port'))
            self.term = max(self.term, message.get('term', 0))
            if self.role != NodeRole.SECONDARY:
                self.role = NodeRole.SECONDARY
            return {'status': 'OK'}
        
        elif msg_type == 'VOTE_REQUEST':
            # Handle vote request
            candidate_term = message.get('term', 0)
            candidate_id = message.get('candidate_id')
            
            if candidate_term > self.term:
                self.term = candidate_term
                self.voted_for = None
            
            if self.voted_for is None or self.voted_for == candidate_id:
                self.voted_for = candidate_id
                return {'status': 'OK', 'vote_granted': True, 'term': self.term}
            else:
                return {'status': 'OK', 'vote_granted': False, 'term': self.term}
        
        elif msg_type == 'REPLICATE':
            # Replicate operation from primary
            if self.role == NodeRole.SECONDARY:
                original_op = message.get('original_operation')
                # Apply without replicating again
                self._apply_operation_locally(original_op)
            return {'status': 'OK'}
        
        return {'status': 'ERROR', 'message': 'Unknown cluster message'}
    
    def _apply_operation_locally(self, operation: Dict[str, Any]):
        """Apply operation locally without replication"""
        op_type = operation.get('operation')
        
        if op_type == 'SET':
            self.kvstore.set(operation['key'], operation['value'], replicate=False)
        elif op_type == 'DELETE':
            self.kvstore.delete(operation['key'], replicate=False)
        elif op_type == 'BULK_SET':
            items = [(item['key'], item['value']) for item in operation['items']]
            self.kvstore.bulk_set(items, replicate=False)
    
    def _replicate_operation(self, operation: Dict[str, Any]):
        """Replicate operation to secondary nodes"""
        message = {
            'operation': 'REPLICATE',
            'original_operation': operation
        }
        
        for peer_host, peer_port in self.peers:
            try:
                self._send_to_peer((peer_host, peer_port), message)
            except:
                pass  # Ignore replication failures for now
    
    def _heartbeat_loop(self):
        """Heartbeat and election loop"""
        while self.running:
            time.sleep(1.0)
            
            if self.role == NodeRole.PRIMARY:
                # Send heartbeats to peers
                self._send_heartbeats()
            else:
                # Check if we should start an election
                if time.time() - self.last_heartbeat > self.heartbeat_timeout:
                    self._start_election()
    
    def _send_heartbeats(self):
        """Send heartbeats to all peers"""
        message = {
            'operation': 'HEARTBEAT',
            'term': self.term,
            'primary_host': self.host,
            'primary_port': self.port
        }
        
        for peer_host, peer_port in self.peers:
            try:
                self._send_to_peer((peer_host, peer_port), message)
            except:
                pass
    
    def _start_election(self):
        """Start leader election"""
        print(f"Node {self.node_id} starting election")
        
        with self._lock:
            self.role = NodeRole.CANDIDATE
            self.term += 1
            self.voted_for = self.node_id
            votes = 1  # Vote for self
        
        # Request votes from peers
        message = {
            'operation': 'VOTE_REQUEST',
            'term': self.term,
            'candidate_id': self.node_id
        }
        
        for peer_host, peer_port in self.peers:
            try:
                response = self._send_to_peer((peer_host, peer_port), message)
                if response.get('vote_granted'):
                    votes += 1
            except:
                pass
        
        # Check if won election (majority)
        if votes > (len(self.peers) + 1) // 2:
            with self._lock:
                self.role = NodeRole.PRIMARY
                self.primary_node = (self.host, self.port)
            print(f"Node {self.node_id} became PRIMARY")
        else:
            with self._lock:
                self.role = NodeRole.SECONDARY
    
    def _send_to_peer(self, peer: Tuple[str, int], message: Dict[str, Any]) -> Dict[str, Any]:
        """Send message to peer"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(peer)
            
            message_data = json.dumps(message).encode('utf-8')
            message_length = len(message_data).to_bytes(4, byteorder='big')
            sock.sendall(message_length + message_data)
            
            length_bytes = sock.recv(4)
            if length_bytes:
                response_length = int.from_bytes(length_bytes, byteorder='big')
                response_data = sock.recv(response_length)
                response = json.loads(response_data.decode('utf-8'))
                sock.close()
                return response
            
            sock.close()
            return {'status': 'ERROR'}
        except Exception as e:
            return {'status': 'ERROR', 'message': str(e)}
    
    def shutdown(self):
        """Shutdown node"""
        self.running = False
        self.kvstore.shutdown()
        if self.server_socket:
            self.server_socket.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clustered KVStore Node')
    parser.add_argument('--node-id', type=int, required=True, help='Node ID')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, required=True, help='Port to bind to')
    parser.add_argument('--peers', help='Comma-separated list of peer addresses (host:port)')
    parser.add_argument('--data-dir', required=True, help='Data directory')
    parser.add_argument('--primary', action='store_true', help='Start as primary')
    
    args = parser.parse_args()
    
    # Parse peers
    peers = []
    if args.peers:
        for peer in args.peers.split(','):
            host, port = peer.strip().split(':')
            peers.append((host, int(port)))
    
    node = ClusterNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        peers=peers,
        data_dir=args.data_dir
    )
    
    if args.primary:
        node.role = NodeRole.PRIMARY
        node.primary_node = (args.host, args.port)
        node.last_heartbeat = time.time()
    
    def signal_handler(sig, frame):
        print("\nReceived shutdown signal")
        node.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    node.start()
    
    # Keep main thread alive
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
