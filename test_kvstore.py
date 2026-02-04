#!/usr/bin/env python3
"""
Tests for KVStore
"""

import unittest
import time
import subprocess
import os
import signal
import shutil
from pathlib import Path
from kvstore_client import KVStoreClient
import threading
import random


class TestKVStoreBasic(unittest.TestCase):
    """Basic functionality tests"""
    
    @classmethod
    def setUpClass(cls):
        """Start server before tests"""
        cls.data_dir = "./test_kvstore_data"
        cls.port = 9998
        
        # Clean up any existing data
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
        
        # Start server
        cls.server_process = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(cls.port), '--data-dir', cls.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for server to start
        time.sleep(2)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after tests"""
        if cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait(timeout=5)
        
        # Clean up data
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
    
    def setUp(self):
        """Create client for each test"""
        self.client = KVStoreClient(port=self.port)
    
    def tearDown(self):
        """Close client after each test"""
        self.client.close()
    
    def test_set_then_get(self):
        """Test: Set then Get"""
        key = "test_key_1"
        value = "test_value_1"
        
        # Set
        result = self.client.Set(key, value)
        self.assertTrue(result)
        
        # Get
        retrieved = self.client.Get(key)
        self.assertEqual(retrieved, value)
    
    def test_set_delete_get(self):
        """Test: Set then Delete then Get"""
        key = "test_key_2"
        value = "test_value_2"
        
        # Set
        self.client.Set(key, value)
        
        # Delete
        result = self.client.Delete(key)
        self.assertTrue(result)
        
        # Get should return None
        retrieved = self.client.Get(key)
        self.assertIsNone(retrieved)
    
    def test_get_without_setting(self):
        """Test: Get without setting"""
        key = "nonexistent_key"
        retrieved = self.client.Get(key)
        self.assertIsNone(retrieved)
    
    def test_set_set_get(self):
        """Test: Set then Set (same key) then Get"""
        key = "test_key_3"
        value1 = "value_1"
        value2 = "value_2"
        
        # First set
        self.client.Set(key, value1)
        
        # Second set (overwrite)
        self.client.Set(key, value2)
        
        # Get should return latest value
        retrieved = self.client.Get(key)
        self.assertEqual(retrieved, value2)
    
    def test_bulk_set(self):
        """Test: Bulk set operation"""
        items = [
            ("bulk_key_1", "bulk_value_1"),
            ("bulk_key_2", "bulk_value_2"),
            ("bulk_key_3", "bulk_value_3")
        ]
        
        # Bulk set
        result = self.client.BulkSet(items)
        self.assertTrue(result)
        
        # Verify all were set
        for key, value in items:
            retrieved = self.client.Get(key)
            self.assertEqual(retrieved, value)


class TestKVStorePersistence(unittest.TestCase):
    """Test persistence across restarts"""
    
    def setUp(self):
        """Setup for persistence tests"""
        self.data_dir = "./test_persist_data"
        self.port = 9997
        
        # Clean up any existing data
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
    
    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
    
    def test_set_restart_get(self):
        """Test: Set then exit (gracefully) then Get"""
        key = "persist_key"
        value = "persist_value"
        
        # Start server
        server = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(self.port), '--data-dir', self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        # Set value
        client = KVStoreClient(port=self.port)
        client.Set(key, value)
        client.close()
        
        # Gracefully shutdown
        server.terminate()
        server.wait(timeout=5)
        time.sleep(1)
        
        # Restart server
        server = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(self.port), '--data-dir', self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        # Get value
        client = KVStoreClient(port=self.port)
        retrieved = client.Get(key)
        client.close()
        
        # Verify
        self.assertEqual(retrieved, value)
        
        # Cleanup
        server.terminate()
        server.wait(timeout=5)


class TestKVStoreACID(unittest.TestCase):
    """Test ACID properties"""
    
    @classmethod
    def setUpClass(cls):
        """Start server before tests"""
        cls.data_dir = "./test_acid_data"
        cls.port = 9996
        
        # Clean up any existing data
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
        
        # Start server
        cls.server_process = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(cls.port), '--data-dir', cls.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after tests"""
        if cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait(timeout=5)
        
        # Clean up data
        if os.path.exists(cls.data_dir):
            shutil.rmtree(cls.data_dir)
    
    def test_concurrent_bulk_set_isolation(self):
        """Test: Concurrent bulk set writes touching same keys maintain isolation"""
        num_threads = 5
        num_operations = 10
        shared_keys = [f"shared_key_{i}" for i in range(10)]
        results = {'success': [], 'failures': []}
        lock = threading.Lock()
        
        def bulk_write_thread(thread_id):
            try:
                client = KVStoreClient(port=self.port)
                for i in range(num_operations):
                    # Each thread writes its own value to shared keys
                    items = [(key, f"thread_{thread_id}_op_{i}") for key in shared_keys]
                    success = client.BulkSet(items)
                    with lock:
                        if success:
                            results['success'].append((thread_id, i))
                        else:
                            results['failures'].append((thread_id, i))
                client.close()
            except Exception as e:
                with lock:
                    results['failures'].append((thread_id, str(e)))
        
        # Run concurrent writes
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=bulk_write_thread, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify: All operations should succeed
        self.assertEqual(len(results['failures']), 0, f"Failures: {results['failures']}")
        
        # Verify: All keys should have consistent values from same thread
        client = KVStoreClient(port=self.port)
        values = [client.Get(key) for key in shared_keys]
        client.close()
        
        # All values should be from the same thread and operation
        # (because bulk_set is atomic)
        unique_values = set(values)
        self.assertGreater(len(unique_values), 0)
        
        # Extract thread_id and op_id from values
        for value in values:
            parts = value.split('_')
            thread_id = parts[1]
            op_id = parts[3]
            
            # All keys should have same thread_id and op_id
            self.assertEqual(values[0], value, 
                           "All keys in bulk_set should be updated atomically")
    
    def test_atomic_bulk_set_with_crash(self):
        """Test: Bulk set is completely applied or not at all (after crash)"""
        # This test starts a separate server and crashes it during bulk writes
        test_port = 9995
        test_data_dir = "./test_atomic_crash"
        
        if os.path.exists(test_data_dir):
            shutil.rmtree(test_data_dir)
        
        # Start server
        server = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(test_port), '--data-dir', test_data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        # Prepare large bulk set
        items = [(f"atomic_key_{i}", f"atomic_value_{i}") for i in range(100)]
        
        # Start bulk write in background
        write_complete = threading.Event()
        write_error = []
        
        def do_bulk_write():
            try:
                client = KVStoreClient(port=test_port)
                client.BulkSet(items)
                write_complete.set()
                client.close()
            except Exception as e:
                write_error.append(e)
        
        write_thread = threading.Thread(target=do_bulk_write)
        write_thread.start()
        
        # Kill server immediately (simulate crash during write)
        time.sleep(0.1)
        server.kill()  # SIGKILL (-9)
        server.wait()
        write_thread.join()
        
        # Restart server
        time.sleep(1)
        server = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(test_port), '--data-dir', test_data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        # Check: Either all keys exist or none exist (atomicity)
        client = KVStoreClient(port=test_port)
        results = [client.Get(key) for key, _ in items]
        client.close()
        
        # Count how many keys exist
        existing = [r for r in results if r is not None]
        
        # Should be either 0 (not applied) or 100 (fully applied)
        self.assertIn(len(existing), [0, 100], 
                     f"Bulk set should be atomic: got {len(existing)} keys")
        
        # Cleanup
        server.terminate()
        server.wait(timeout=5)
        if os.path.exists(test_data_dir):
            shutil.rmtree(test_data_dir)


def run_tests():
    """Run all tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestKVStoreBasic))
    suite.addTests(loader.loadTestsFromTestCase(TestKVStorePersistence))
    suite.addTests(loader.loadTestsFromTestCase(TestKVStoreACID))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
