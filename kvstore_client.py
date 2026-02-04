"""
Client for KVStore Server
"""

import socket
import json
from typing import Any, Optional, List, Tuple


class KVStoreClient:
    """Client for connecting to KVStore server"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999):
        self.host = host
        self.port = port
        self.socket = None
        self._connect()
    
    def _connect(self):
        """Connect to the server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def _send_request(self, request: dict) -> dict:
        """Send request and receive response"""
        # Serialize request
        request_data = json.dumps(request).encode('utf-8')
        request_length = len(request_data).to_bytes(4, byteorder='big')
        
        # Send length-prefixed request
        self.socket.sendall(request_length + request_data)
        
        # Receive length-prefixed response
        length_bytes = self.socket.recv(4)
        if not length_bytes:
            raise ConnectionError("Connection closed by server")
        
        response_length = int.from_bytes(length_bytes, byteorder='big')
        response_data = b''
        while len(response_data) < response_length:
            chunk = self.socket.recv(min(response_length - len(response_data), 4096))
            if not chunk:
                raise ConnectionError("Connection closed by server")
            response_data += chunk
        
        return json.loads(response_data.decode('utf-8'))
    
    def Set(self, key: str, value: Any) -> bool:
        """Set a key-value pair"""
        request = {
            'operation': 'SET',
            'key': key,
            'value': value
        }
        response = self._send_request(request)
        return response.get('status') == 'OK' and response.get('success', False)
    
    def Get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        request = {
            'operation': 'GET',
            'key': key
        }
        response = self._send_request(request)
        if response.get('status') == 'NOT_FOUND':
            return None
        return response.get('value')
    
    def Delete(self, key: str) -> bool:
        """Delete a key"""
        request = {
            'operation': 'DELETE',
            'key': key
        }
        response = self._send_request(request)
        return response.get('status') == 'OK' and response.get('success', False)
    
    def BulkSet(self, items: List[Tuple[str, Any]]) -> bool:
        """Bulk set operation"""
        request = {
            'operation': 'BULK_SET',
            'items': [{'key': k, 'value': v} for k, v in items]
        }
        response = self._send_request(request)
        return response.get('status') == 'OK' and response.get('success', False)
    
    def close(self):
        """Close connection"""
        if self.socket:
            self.socket.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Convenience function to create client
def create_client(host: str = 'localhost', port: int = 9999) -> KVStoreClient:
    """Create a new client instance"""
    return KVStoreClient(host, port)
