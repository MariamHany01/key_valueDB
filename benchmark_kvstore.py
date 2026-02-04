"""
Benchmarks for KVStore
Tests write throughput and durability
"""

import time
import subprocess
import os
import shutil
import threading
import random
from typing import List, Set
from kvstore_client import KVStoreClient
import signal


class ThroughputBenchmark:
    """Benchmark write throughput"""
    
    def __init__(self, port: int = 9999):
        self.port = port
    
    def run(self, num_writes: int = 10000, prepopulate: int = 0):
        """
        Run throughput benchmark
        Args:
            num_writes: Number of writes to perform
            prepopulate: Number of keys to prepopulate
        """
        print(f"\n{'='*60}")
        print(f"THROUGHPUT BENCHMARK")
        print(f"Writes: {num_writes}, Prepopulated: {prepopulate}")
        print(f"{'='*60}")
        
        client = KVStoreClient(port=self.port)
        
        # Prepopulate if requested
        if prepopulate > 0:
            print(f"Prepopulating {prepopulate} keys...")
            for i in range(prepopulate):
                client.Set(f"prepop_key_{i}", f"prepop_value_{i}")
            print("Prepopulation complete")
        
        # Benchmark writes
        print(f"Performing {num_writes} writes...")
        start_time = time.time()
        
        for i in range(num_writes):
            key = f"bench_key_{i}"
            value = f"bench_value_{i}"
            client.Set(key, value)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_writes / duration
        
        print(f"Duration: {duration:.2f} seconds")
        print(f"Throughput: {throughput:.2f} writes/second")
        print(f"Average latency: {(duration/num_writes)*1000:.2f} ms/write")
        
        client.close()
        
        return {
            'num_writes': num_writes,
            'prepopulated': prepopulate,
            'duration': duration,
            'throughput': throughput
        }


class DurabilityBenchmark:
    """Benchmark durability under crashes"""
    
    def __init__(self, port: int = 9994, data_dir: str = "./bench_durability_data"):
        self.port = port
        self.data_dir = data_dir
        self.acknowledged_keys: Set[str] = set()
        self.lock = threading.Lock()
        self.server_process = None
    
    def start_server(self):
        """Start the server"""
        self.server_process = subprocess.Popen(
            ['python3', 'kvstore_server.py', '--port', str(self.port), '--data-dir', self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
    
    def kill_server(self):
        """Kill server with SIGKILL"""
        if self.server_process:
            self.server_process.kill()  # SIGKILL (-9)
            self.server_process.wait()
            self.server_process = None
    
    def writer_thread(self, duration: int = 10):
        """Thread that continuously writes data"""
        try:
            client = KVStoreClient(port=self.port)
            counter = 0
            end_time = time.time() + duration
            
            while time.time() < end_time:
                try:
                    key = f"durable_key_{counter}"
                    value = f"durable_value_{counter}"
                    
                    success = client.Set(key, value)
                    if success:
                        with self.lock:
                            self.acknowledged_keys.add(key)
                        counter += 1
                except:
                    break
            
            client.close()
        except Exception as e:
            print(f"Writer error: {e}")
    
    def killer_thread(self, duration: int = 10, kill_interval: float = 2.0):
        """Thread that randomly kills the server"""
        end_time = time.time() + duration
        
        while time.time() < end_time:
            time.sleep(kill_interval)
            if time.time() < end_time:
                print(f"Killing server at {time.time():.2f}")
                self.kill_server()
                time.sleep(0.5)
                print(f"Restarting server at {time.time():.2f}")
                self.start_server()
    
    def run(self, duration: int = 10, kill_interval: float = 2.0):
        """
        Run durability benchmark
        Args:
            duration: How long to run the test
            kill_interval: How often to kill the server
        """
        print(f"\n{'='*60}")
        print(f"DURABILITY BENCHMARK")
        print(f"Duration: {duration}s, Kill interval: {kill_interval}s")
        print(f"{'='*60}")
        
        # Clean up
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        
        # Start server
        self.start_server()
        
        # Start writer thread
        writer = threading.Thread(target=self.writer_thread, args=(duration,))
        
        # Start killer thread
        killer = threading.Thread(target=self.killer_thread, args=(duration, kill_interval))
        
        writer.start()
        time.sleep(0.5)  # Let writer start first
        killer.start()
        
        # Wait for completion
        writer.join()
        killer.join()
        
        # Final server state
        if self.server_process:
            self.kill_server()
        
        # Restart server one last time to check persistence
        time.sleep(1)
        self.start_server()
        
        # Check which acknowledged keys are present
        print(f"\nChecking durability...")
        print(f"Total acknowledged writes: {len(self.acknowledged_keys)}")
        
        client = KVStoreClient(port=self.port)
        lost_keys = []
        
        for key in self.acknowledged_keys:
            value = client.Get(key)
            if value is None:
                lost_keys.append(key)
        
        client.close()
        
        # Cleanup
        self.kill_server()
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        
        # Results
        durability_rate = ((len(self.acknowledged_keys) - len(lost_keys)) / 
                          len(self.acknowledged_keys) * 100 if self.acknowledged_keys else 0)
        
        print(f"Lost keys: {len(lost_keys)}")
        print(f"Durability rate: {durability_rate:.2f}%")
        
        return {
            'total_writes': len(self.acknowledged_keys),
            'lost_keys': len(lost_keys),
            'durability_rate': durability_rate
        }


def run_all_benchmarks():
    """Run all benchmarks"""
    # Setup
    data_dir = "./bench_kvstore_data"
    port = 9993
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    # Start server for throughput tests
    print("Starting server for benchmarks...")
    server = subprocess.Popen(
        ['python3', 'kvstore_server.py', '--port', str(port), '--data-dir', data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    
    try:
        # Throughput benchmarks
        throughput_bench = ThroughputBenchmark(port=port)
        
        print("\n" + "="*60)
        print("THROUGHPUT BENCHMARKS")
        print("="*60)
        
        # Test 1: Empty database
        result1 = throughput_bench.run(num_writes=1000, prepopulate=0)
        
        # Test 2: With 10k prepopulated keys
        result2 = throughput_bench.run(num_writes=1000, prepopulate=10000)
        
        # Test 3: With 50k prepopulated keys
        result3 = throughput_bench.run(num_writes=1000, prepopulate=50000)
        
        print("\n" + "="*60)
        print("THROUGHPUT SUMMARY")
        print("="*60)
        print(f"Empty DB: {result1['throughput']:.2f} writes/sec")
        print(f"10K keys: {result2['throughput']:.2f} writes/sec")
        print(f"50K keys: {result3['throughput']:.2f} writes/sec")
        
    finally:
        # Stop server
        server.terminate()
        server.wait(timeout=5)
    
    # Durability benchmark (manages its own server)
    durability_bench = DurabilityBenchmark()
    result4 = durability_bench.run(duration=15, kill_interval=2.0)
    
    # Cleanup
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    print("\n" + "="*60)
    print("BENCHMARK SUMMARY")
    print("="*60)
    print(f"Throughput (empty): {result1['throughput']:.2f} writes/sec")
    print(f"Throughput (10K): {result2['throughput']:.2f} writes/sec")
    print(f"Throughput (50K): {result3['throughput']:.2f} writes/sec")
    print(f"Durability rate: {result4['durability_rate']:.2f}%")
    print(f"Lost keys: {result4['lost_keys']}/{result4['total_writes']}")


if __name__ == '__main__':
    run_all_benchmarks()
