"""
Persistent Key-Value Store Server with TCP Interface
Implements Write-Ahead Logging (WAL) for 100% durability
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


class WriteAheadLog:
    """Write-Ahead Log for ensuring durability"""
    
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.wal_file = None
        self._lock = threading.Lock()
        self._open_wal()
    
    def _open_wal(self):
        """Open WAL file in append mode"""
        self.wal_file = open(self.wal_path, 'ab', buffering=0)  # No buffering for immediate writes
    
    def log_operation(self, operation: Dict[str, Any]) -> None:
        """Log an operation to WAL with immediate sync"""
        with self._lock:
            entry = pickle.dumps(operation)
            # Write length prefix for easy reading
            length = len(entry).to_bytes(4, byteorder='big')
            self.wal_file.write(length + entry)
            # Force OS to write to disk (fsync for durability)
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
            # Truncate the WAL
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
        
        # Initialize WAL
        self.wal = WriteAheadLog(str(self.wal_file))
        
        # Load data from disk
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load data from persistent storage and replay WAL"""
        # Load checkpoint
        if self.data_file.exists():
            with open(self.data_file, 'rb') as f:
                self.store = pickle.load(f)
            print(f"Loaded {len(self.store)} keys from checkpoint")
        
        # Replay WAL
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
    
    def set(self, key: str, value: Any) -> bool:
        """Set a key-value pair"""
        with self._lock:
            # Log to WAL first (durability)
            self.wal.log_operation({'type': 'set', 'key': key, 'value': value})
            # Then update in-memory
            self.store[key] = value
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        with self._lock:
            return self.store.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a key"""
        with self._lock:
            if key not in self.store:
                return False
            # Log to WAL first
            self.wal.log_operation({'type': 'delete', 'key': key})
            # Then delete from memory
            del self.store[key]
            return True
    
    def bulk_set(self, items: List[Tuple[str, Any]]) -> bool:
        """Bulk set operation (atomic)"""
        with self._lock:
            # Log entire bulk operation as single atomic entry
            self.wal.log_operation({'type': 'bulk_set', 'items': items})
            # Apply all changes
            for key, value in items:
                self.store[key] = value
            return True
    
    def get_all_data(self) -> Dict[str, Any]:
        """Get all data (for indexing/replication)"""
        with self._lock:
            return dict(self.store)
    
    def checkpoint(self):
        """Create a checkpoint and truncate WAL"""
        with self._lock:
            # Write current state to disk
            with open(self.data_file, 'wb') as f:
                pickle.dump(self.store, f)
            # Truncate WAL
            self.wal.checkpoint()
            print(f"Checkpoint created with {len(self.store)} keys")
    
    def shutdown(self):
        """Graceful shutdown"""
        print("Shutting down KVStore...")
        self.checkpoint()
        self.wal.close()
        print("Shutdown complete")


class KVStoreServer:
    """TCP Server for KVStore"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999, data_dir: str = "./kvstore_data"):
        self.host = host
        self.port = port
        self.kvstore = KVStore(data_dir)
        self.server_socket = None
        self.running = False
        self.checkpoint_interval = 60  # Checkpoint every 60 seconds
        self.checkpoint_thread = None
    
    def start(self):
        """Start the TCP server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"KVStore server started on {self.host}:{self.port}")
        
        # Start checkpoint thread
        self.checkpoint_thread = threading.Thread(target=self._periodic_checkpoint, daemon=True)
        self.checkpoint_thread.start()
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")
    
    def _periodic_checkpoint(self):
        """Periodically checkpoint data"""
        while self.running:
            time.sleep(self.checkpoint_interval)
            if self.running:
                self.kvstore.checkpoint()
    
    def _handle_client(self, client_socket: socket.socket, address):
        """Handle client connection"""
        try:
            while True:
                # Receive length-prefixed message
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
                
                # Send length-prefixed response
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
            
            if operation == 'SET':
                key = request['key']
                value = request['value']
                success = self.kvstore.set(key, value)
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
                return {'status': 'OK', 'success': success}
            
            elif operation == 'BULK_SET':
                items = [(item['key'], item['value']) for item in request['items']]
                success = self.kvstore.bulk_set(items)
                return {'status': 'OK', 'success': success}
            
            else:
                return {'status': 'ERROR', 'message': 'Unknown operation'}
        
        except Exception as e:
            return {'status': 'ERROR', 'message': str(e)}
    
    def shutdown(self):
        """Shutdown server"""
        self.running = False
        self.kvstore.shutdown()
        if self.server_socket:
            self.server_socket.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='KVStore Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=9999, help='Port to bind to')
    parser.add_argument('--data-dir', default='./kvstore_data', help='Data directory')
    
    args = parser.parse_args()
    
    server = KVStoreServer(host=args.host, port=args.port, data_dir=args.data_dir)
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        print("\nReceived shutdown signal")
        server.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    server.start()


if __name__ == '__main__':
    main()
