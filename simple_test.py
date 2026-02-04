#!/usr/bin/env python3
"""
Simple manual test for KVStore
Run this AFTER starting the server manually in another terminal
"""

from kvstore_client import KVStoreClient
import sys


def test_basic_operations():
    """Test basic operations"""
    print("\n" + "="*60)
    print("Testing Basic Operations")
    print("="*60)
    
    try:
        print("\n1. Connecting to server on port 9999...")
        client = KVStoreClient(host='localhost', port=9999)
        print("✓ Connected successfully")
        
        print("\n2. Testing SET operation...")
        client.Set('test_key_1', 'test_value_1')
        print("✓ SET successful")
        
        print("\n3. Testing GET operation...")
        value = client.Get('test_key_1')
        print(f"✓ GET successful: {value}")
        assert value == 'test_value_1', "Value mismatch!"
        
        print("\n4. Testing SET with overwrite...")
        client.Set('test_key_1', 'new_value')
        value = client.Get('test_key_1')
        print(f"✓ Overwrite successful: {value}")
        assert value == 'new_value', "Value mismatch!"
        
        print("\n5. Testing DELETE operation...")
        client.Delete('test_key_1')
        value = client.Get('test_key_1')
        print(f"✓ DELETE successful: {value}")
        assert value is None, "Key should not exist!"
        
        print("\n6. Testing BULK SET operation...")
        items = [
            ('bulk_key_1', 'bulk_value_1'),
            ('bulk_key_2', 'bulk_value_2'),
            ('bulk_key_3', 'bulk_value_3')
        ]
        client.BulkSet(items)
        print("✓ BULK SET successful")
        
        print("\n7. Verifying bulk set results...")
        for key, expected_value in items:
            value = client.Get(key)
            print(f"   {key}: {value}")
            assert value == expected_value, f"Value mismatch for {key}!"
        print("✓ All bulk values verified")
        
        print("\n8. Testing with complex data types...")
        client.Set('user:1', {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'})
        user = client.Get('user:1')
        print(f"✓ Complex data: {user}")
        assert user['name'] == 'Alice', "User data mismatch!"
        
        client.close()
        print("\n" + "="*60)
        print("✓ ALL TESTS PASSED!")
        print("="*60)
        
    except ConnectionRefusedError:
        print("\n✗ ERROR: Could not connect to server!")
        print("\nPlease start the server first:")
        print("  python kvstore_server.py --port 9999 --data-dir ./test_data")
        print("\nThen run this test again:")
        print("  python simple_test.py")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def test_persistence():
    """Test persistence (requires server restart)"""
    print("\n" + "="*60)
    print("Testing Persistence")
    print("="*60)
    print("\nThis test requires manual steps:")
    print("\n1. Make sure server is running")
    print("2. This script will write some data")
    print("3. Stop the server (Ctrl+C)")
    print("4. Restart the server")
    print("5. Run this script again to verify data persisted")
    
    input("\nPress Enter to continue...")
    
    try:
        client = KVStoreClient(port=9999)
        
        # Check if data already exists (from previous run)
        test_value = client.Get('persist_test_key')
        
        if test_value is None:
            print("\nFirst run: Writing test data...")
            client.Set('persist_test_key', 'persistent_value')
            print("✓ Data written: persist_test_key = persistent_value")
            print("\nNow:")
            print("1. Stop the server (Ctrl+C in server terminal)")
            print("2. Restart the server")
            print("3. Run this script again to verify persistence")
        else:
            print("\nSecond run: Checking persisted data...")
            print(f"✓ Data recovered: persist_test_key = {test_value}")
            if test_value == 'persistent_value':
                print("✓ PERSISTENCE TEST PASSED!")
            else:
                print("✗ Value mismatch!")
        
        client.close()
        
    except ConnectionRefusedError:
        print("\n✗ ERROR: Could not connect to server!")
        print("Please start the server first.")
        sys.exit(1)


def main():
    """Main test runner"""
    print("\n" + "="*60)
    print("KVStore Simple Test")
    print("="*60)
    print("\nBefore running this test, start the server in another terminal:")
    print("  python kvstore_server.py --port 9999 --data-dir ./test_data")
    print("\nPress Ctrl+C to cancel, or Enter to continue...")
    
    try:
        input()
    except KeyboardInterrupt:
        print("\nCancelled")
        sys.exit(0)
    
    # Run tests
    test_basic_operations()
    
    print("\n" + "="*60)
    print("Would you like to test persistence? (requires server restart)")
    response = input("Enter 'yes' to test persistence: ")
    
    if response.lower() in ['yes', 'y']:
        test_persistence()


if __name__ == '__main__':
    main()
