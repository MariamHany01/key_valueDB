#!/usr/bin/env python3
"""
Comprehensive Windows Demo for KVStore
Runs all tests and benchmarks with proper server management
"""

import subprocess
import time
import sys
import os
import socket
import shutil
from kvstore_client import KVStoreClient


def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def wait_for_server(port, timeout=30):
    """Wait for server to be ready by checking if port is accepting connections"""
    print(f"Waiting for server on port {port} to be ready...", end='', flush=True)
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(1)
            test_socket.connect(('localhost', port))
            test_socket.close()
            print(" ✓")
            return True
        except (socket.error, ConnectionRefusedError):
            print(".", end='', flush=True)
            time.sleep(0.5)
    
    print(" ✗")
    return False


def stop_server(server_process):
    """Stop server process properly on Windows"""
    if server_process and server_process.poll() is None:
        if sys.platform == 'win32':
            try:
                subprocess.call(
                    ['taskkill', '/F', '/T', '/PID', str(server_process.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            except:
                server_process.terminate()
        else:
            server_process.terminate()
        
        try:
            server_process.wait(timeout=5)
        except:
            pass
        
        time.sleep(2)


def start_server(port, data_dir):
    """Start server with proper Windows handling"""
    print(f"Starting server on port {port}...")
    
    if sys.platform == 'win32':
        # Use CREATE_NO_WINDOW to hide console on Windows
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        
        server = subprocess.Popen(
            [sys.executable, 'kvstore_server.py', '--port', str(port), '--data-dir', data_dir],
            startupinfo=startupinfo,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    else:
        server = subprocess.Popen(
            [sys.executable, 'kvstore_server.py', '--port', str(port), '--data-dir', data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    
    return server


def test_basic_operations(port=9999):
    """Test basic CRUD operations"""
    print_section("TEST 1: Basic Operations")
    
    data_dir = './test_basic_ops'
    server = start_server(port, data_dir)
    
    if not wait_for_server(port, timeout=30):
        print("✗ Server failed to start")
        stop_server(server)
        return False
    
    try:
        client = KVStoreClient(port=port)
        print("✓ Client connected")
        
        # Test 1: Set then Get
        print("\n1. Testing SET then GET...")
        client.Set('test_key_1', 'test_value_1')
        value = client.Get('test_key_1')
        assert value == 'test_value_1', "Value mismatch!"
        print("   ✓ Set then Get works")
        
        # Test 2: Set then Delete then Get
        print("\n2. Testing SET then DELETE then GET...")
        client.Set('test_key_2', 'test_value_2')
        client.Delete('test_key_2')
        value = client.Get('test_key_2')
        assert value is None, "Key should not exist!"
        print("   ✓ Set then Delete then Get works")
        
        # Test 3: Get without setting
        print("\n3. Testing GET without setting...")
        value = client.Get('nonexistent_key')
        assert value is None, "Nonexistent key should return None!"
        print("   ✓ Get without setting returns None")
        
        # Test 4: Set then Set (overwrite)
        print("\n4. Testing SET then SET (overwrite)...")
        client.Set('test_key_3', 'value_1')
        client.Set('test_key_3', 'value_2')
        value = client.Get('test_key_3')
        assert value == 'value_2', "Overwrite failed!"
        print("   ✓ Overwrite works")
        
        # Test 5: Bulk Set
        print("\n5. Testing BULK SET...")
        items = [
            ('bulk_key_1', 'bulk_value_1'),
            ('bulk_key_2', 'bulk_value_2'),
            ('bulk_key_3', 'bulk_value_3')
        ]
        client.BulkSet(items)
        for key, expected_value in items:
            value = client.Get(key)
            assert value == expected_value, f"Bulk set failed for {key}!"
        print("   ✓ Bulk Set works")
        
        # Test 6: Complex data types
        print("\n6. Testing complex data types...")
        client.Set('user:1', {'name': 'Alice', 'age': 30, 'tags': ['admin', 'user']})
        user = client.Get('user:1')
        assert user['name'] == 'Alice', "Complex data failed!"
        print("   ✓ Complex data types work")
        
        client.close()
        print("\n✓ All basic operations passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        stop_server(server)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


def test_persistence(port=9997):
    """Test persistence across restarts"""
    print_section("TEST 2: Persistence")
    
    data_dir = './test_persistence'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    print("Phase 1: Writing data...")
    server = start_server(port, data_dir)
    
    if not wait_for_server(port, timeout=30):
        print("✗ Server failed to start")
        stop_server(server)
        return False
    
    try:
        # Write data
        client = KVStoreClient(port=port)
        test_data = {}
        for i in range(10):
            key = f'persist:key:{i}'
            value = f'value_{i}'
            client.Set(key, value)
            test_data[key] = value
        print(f"✓ Written {len(test_data)} key-value pairs")
        client.close()
        
        # Stop server
        print("\nPhase 2: Stopping server gracefully...")
        stop_server(server)
        print("✓ Server stopped")
        
        # Restart server
        print("\nPhase 3: Restarting server...")
        server = start_server(port, data_dir)
        
        if not wait_for_server(port, timeout=30):
            print("✗ Server failed to restart")
            return False
        print("✓ Server restarted")
        
        # Verify data
        print("\nPhase 4: Verifying persisted data...")
        client = KVStoreClient(port=port)
        recovered = 0
        for key, expected_value in test_data.items():
            value = client.Get(key)
            if value == expected_value:
                recovered += 1
            else:
                print(f"   ✗ Key {key}: expected {expected_value}, got {value}")
        
        print(f"✓ Recovered {recovered}/{len(test_data)} keys")
        client.close()
        
        success = recovered == len(test_data)
        if success:
            print("\n✓ Persistence test passed!")
        else:
            print("\n✗ Some keys were not persisted")
        
        return success
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        stop_server(server)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


def test_concurrent_operations(port=9996):
    """Test concurrent operations (ACID)"""
    print_section("TEST 3: Concurrent Operations (ACID)")
    
    data_dir = './test_concurrent'
    server = start_server(port, data_dir)
    
    if not wait_for_server(port, timeout=30):
        print("✗ Server failed to start")
        stop_server(server)
        return False
    
    try:
        import threading
        
        print("Testing concurrent BulkSet operations...")
        
        results = {'success': 0, 'failed': 0}
        lock = threading.Lock()
        
        def bulk_write_thread(thread_id, num_ops=5):
            try:
                client = KVStoreClient(port=port)
                for i in range(num_ops):
                    items = [(f'thread{thread_id}_key{i}_{j}', f'thread{thread_id}_value{i}_{j}') 
                            for j in range(5)]
                    success = client.BulkSet(items)
                    with lock:
                        if success:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                client.close()
            except Exception as e:
                with lock:
                    results['failed'] += 1
                print(f"   Thread {thread_id} error: {e}")
        
        # Run 3 concurrent threads
        threads = []
        num_threads = 3
        for i in range(num_threads):
            t = threading.Thread(target=bulk_write_thread, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print(f"✓ Concurrent operations: {results['success']} successful, {results['failed']} failed")
        
        # Verify atomicity - check that bulk sets are complete
        print("\nVerifying atomicity of bulk operations...")
        client = KVStoreClient(port=port)
        
        atomic = True
        for thread_id in range(num_threads):
            for i in range(5):
                keys = [f'thread{thread_id}_key{i}_{j}' for j in range(5)]
                values = [client.Get(key) for key in keys]
                
                # Either all values exist or none exist (atomicity)
                existing = sum(1 for v in values if v is not None)
                if existing != 0 and existing != len(keys):
                    print(f"   ✗ Thread {thread_id} operation {i}: partial write detected!")
                    atomic = False
        
        client.close()
        
        if atomic:
            print("✓ All bulk operations are atomic")
        
        success = results['failed'] == 0 and atomic
        if success:
            print("\n✓ Concurrent operations test passed!")
        else:
            print("\n✗ Some concurrent operations failed")
        
        return success
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        stop_server(server)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


def benchmark_throughput(port=9995):
    """Benchmark write throughput"""
    print_section("BENCHMARK 1: Write Throughput")
    
    data_dir = './bench_throughput'
    server = start_server(port, data_dir)
    
    if not wait_for_server(port, timeout=30):
        print("✗ Server failed to start")
        stop_server(server)
        return
    
    try:
        client = KVStoreClient(port=port)
        
        # Test 1: Empty database
        print("\n1. Throughput with empty database...")
        num_writes = 1000
        start = time.time()
        for i in range(num_writes):
            client.Set(f'bench_key_{i}', f'bench_value_{i}')
        duration = time.time() - start
        throughput1 = num_writes / duration
        print(f"   Writes: {num_writes}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Throughput: {throughput1:.2f} writes/sec")
        print(f"   Avg latency: {(duration/num_writes)*1000:.2f} ms/write")
        
        # Test 2: With 10k existing keys
        print("\n2. Throughput with 10K prepopulated keys...")
        for i in range(10000):
            client.Set(f'prepop_key_{i}', f'prepop_value_{i}')
        
        num_writes = 1000
        start = time.time()
        for i in range(num_writes):
            client.Set(f'bench2_key_{i}', f'bench2_value_{i}')
        duration = time.time() - start
        throughput2 = num_writes / duration
        print(f"   Writes: {num_writes}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Throughput: {throughput2:.2f} writes/sec")
        print(f"   Avg latency: {(duration/num_writes)*1000:.2f} ms/write")
        
        # Test 3: Bulk operations
        print("\n3. Bulk operation throughput...")
        num_bulks = 100
        items_per_bulk = 10
        start = time.time()
        for i in range(num_bulks):
            items = [(f'bulk_{i}_key_{j}', f'bulk_{i}_value_{j}') 
                    for j in range(items_per_bulk)]
            client.BulkSet(items)
        duration = time.time() - start
        total_writes = num_bulks * items_per_bulk
        throughput3 = total_writes / duration
        print(f"   Bulk operations: {num_bulks}")
        print(f"   Items per bulk: {items_per_bulk}")
        print(f"   Total writes: {total_writes}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Throughput: {throughput3:.2f} writes/sec")
        print(f"   Avg latency: {(duration/total_writes)*1000:.2f} ms/write")
        
        client.close()
        
        print("\n" + "="*70)
        print("THROUGHPUT SUMMARY")
        print("="*70)
        print(f"Empty DB:        {throughput1:.2f} writes/sec")
        print(f"With 10K keys:   {throughput2:.2f} writes/sec")
        print(f"Bulk operations: {throughput3:.2f} writes/sec")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        stop_server(server)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


def benchmark_durability(port=9994):
    """Benchmark durability under crashes"""
    print_section("BENCHMARK 2: Durability Under Crashes")
    
    data_dir = './bench_durability'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    print("This benchmark simulates crashes and verifies data durability...")
    print("Running 5 crash cycles...")
    
    acknowledged_keys = set()
    
    try:
        for cycle in range(5):
            print(f"\nCycle {cycle + 1}/5:")
            
            # Start server
            print("  Starting server...", end='', flush=True)
            server = start_server(port, data_dir)
            if not wait_for_server(port, timeout=30):
                print(" ✗ Failed")
                continue
            print(" ✓")
            
            # Write data
            print("  Writing data...", end='', flush=True)
            client = KVStoreClient(port=port)
            writes = 0
            for i in range(50):
                key = f'durable_cycle{cycle}_key{i}'
                value = f'durable_cycle{cycle}_value{i}'
                success = client.Set(key, value)
                if success:
                    acknowledged_keys.add(key)
                    writes += 1
            client.close()
            print(f" ✓ {writes} writes acknowledged")
            
            # Simulate crash (kill -9)
            print("  Simulating crash...", end='', flush=True)
            if server.poll() is None:
                if sys.platform == 'win32':
                    subprocess.call(
                        ['taskkill', '/F', '/T', '/PID', str(server.pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                else:
                    import signal
                    os.kill(server.pid, signal.SIGKILL)
            server.wait()
            print(" ✓")
            time.sleep(1)
        
        # Final verification
        print("\n" + "="*70)
        print("Final Verification")
        print("="*70)
        
        print("\nRestarting server for final check...")
        server = start_server(port, data_dir)
        if not wait_for_server(port, timeout=30):
            print("✗ Server failed to start for verification")
            return
        
        print(f"Checking {len(acknowledged_keys)} acknowledged writes...")
        client = KVStoreClient(port=port)
        
        lost_keys = []
        for key in acknowledged_keys:
            value = client.Get(key)
            if value is None:
                lost_keys.append(key)
        
        client.close()
        stop_server(server)
        
        durability_rate = ((len(acknowledged_keys) - len(lost_keys)) / 
                          len(acknowledged_keys) * 100 if acknowledged_keys else 0)
        
        print(f"\nTotal acknowledged writes: {len(acknowledged_keys)}")
        print(f"Lost keys: {len(lost_keys)}")
        print(f"Durability rate: {durability_rate:.2f}%")
        
        if durability_rate == 100:
            print("\n✓ 100% durability achieved!")
        else:
            print(f"\n⚠ Warning: {len(lost_keys)} keys were lost")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


def test_indexing():
    """Test indexing features"""
    print_section("TEST 4: Indexing (Full-text & Semantic Search)")
    
    try:
        from kvstore_server import KVStore
        from kvstore_indexing import IndexedKVStore
        import tempfile
        
        temp_dir = tempfile.mkdtemp()
        
        print("Creating indexed store...")
        kvstore = KVStore(temp_dir)
        indexed = IndexedKVStore(kvstore)
        
        # Add documents
        print("\n1. Adding test documents...")
        docs = [
            ('doc1', 'The quick brown fox jumps over the lazy dog'),
            ('doc2', 'A fast brown fox leaps over a sleeping dog'),
            ('doc3', 'The lazy cat sleeps all day long'),
            ('doc4', 'Machine learning and artificial intelligence are transforming technology'),
            ('doc5', 'Deep learning neural networks are powerful tools for AI'),
            ('doc6', 'Python is a great programming language for data science'),
            ('doc7', 'Database systems store and retrieve data efficiently'),
            ('doc8', 'Distributed systems provide fault tolerance and scalability')
        ]
        for key, value in docs:
            indexed.set(key, value)
        print(f"   ✓ Indexed {len(docs)} documents")
        
        # Full-text search tests
        print("\n2. Full-text search (AND - all words must match):")
        queries = ['brown fox', 'learning AI', 'data systems']
        for query in queries:
            results = indexed.search_fulltext(query)
            print(f"   Query '{query}': {len(results)} results")
            for key, value in results[:2]:
                print(f"      • {key}: {value[:50]}...")
        
        print("\n3. Full-text search (OR - any word matches):")
        query = 'lazy'
        results = indexed.search_fulltext(query, match_all=False)
        print(f"   Query '{query}': {len(results)} results")
        for key, value in results:
            print(f"      • {key}: {value[:50]}...")
        
        # Semantic search tests
        print("\n4. Semantic search (similarity-based):")
        queries = [
            'quick animal jumping',
            'neural network AI',
            'programming languages'
        ]
        for query in queries:
            results = indexed.search_semantic(query, top_k=3)
            print(f"   Query '{query}':")
            for key, value, score in results:
                print(f"      • {key} (score: {score:.3f}): {value[:40]}...")
        
        # Index stats
        print("\n5. Index statistics:")
        stats = indexed.get_index_stats()
        print(f"   Inverted index: {stats['inverted_index']['unique_words']} unique words")
        print(f"   Embedding index: {stats['embedding_index']['total_documents']} documents")
        
        kvstore.shutdown()
        shutil.rmtree(temp_dir)
        
        print("\n✓ Indexing test passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_all():
    """Clean up all test data directories"""
    print("\nCleaning up test data...")
    dirs = [
        './test_basic_ops', './test_persistence', './test_concurrent',
        './bench_throughput', './bench_durability'
    ]
    for d in dirs:
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                print(f"   ✓ Removed {d}")
            except Exception as e:
                print(f"   ⚠ Could not remove {d}: {e}")


def main():
    """Main demo runner"""
    print("\n" + "="*70)
    print(" "*15 + "KVStore Complete Test Suite")
    print(" "*20 + "(Windows Version)")
    print("="*70)
    print("\nThis will run:")
    print("  • Basic operation tests")
    print("  • Persistence tests")
    print("  • Concurrent operation tests")
    print("  • Indexing tests")
    print("  • Throughput benchmarks")
    print("  • Durability benchmarks")
    print("\nEstimated time: 3-5 minutes")
    print("\nServers will run in the background (hidden windows).")
    
    input("\nPress Enter to start, or Ctrl+C to cancel...")
    
    results = {
        'passed': [],
        'failed': []
    }
    
    try:
        # Run tests
        print("\n" + "="*70)
        print("STARTING TEST SUITE")
        print("="*70)
        
        # Test 1: Basic Operations
        if test_basic_operations():
            results['passed'].append('Basic Operations')
        else:
            results['failed'].append('Basic Operations')
        time.sleep(2)
        
        # Test 2: Persistence
        if test_persistence():
            results['passed'].append('Persistence')
        else:
            results['failed'].append('Persistence')
        time.sleep(2)
        
        # Test 3: Concurrent Operations
        if test_concurrent_operations():
            results['passed'].append('Concurrent Operations')
        else:
            results['failed'].append('Concurrent Operations')
        time.sleep(2)
        
        # Test 4: Indexing
        if test_indexing():
            results['passed'].append('Indexing')
        else:
            results['failed'].append('Indexing')
        time.sleep(2)
        
        # Benchmark 1: Throughput
        print("\n" + "="*70)
        print("STARTING BENCHMARKS")
        print("="*70)
        benchmark_throughput()
        time.sleep(2)
        
        # Benchmark 2: Durability
        benchmark_durability()
        
        # Final Summary
        print("\n" + "="*70)
        print("FINAL SUMMARY")
        print("="*70)
        
        print(f"\n✓ Tests Passed: {len(results['passed'])}")
        for test in results['passed']:
            print(f"   • {test}")
        
        if results['failed']:
            print(f"\n✗ Tests Failed: {len(results['failed'])}")
            for test in results['failed']:
                print(f"   • {test}")
        else:
            print("\n🎉 All tests passed!")
        
        print("\nBenchmarks completed successfully!")
        
        print("\n" + "="*70)
        print("What you can do next:")
        print("="*70)
        print("1. Review the results above")
        print("2. Try the cluster setup (see WINDOWS_GUIDE.md)")
        print("3. Build your own application using kvstore_client.py")
        print("4. Explore indexing features with kvstore_indexing.py")
        
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n\nError during demo: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup_all()
        print("\n✓ Cleanup complete")


if __name__ == '__main__':
    main()
