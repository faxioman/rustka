#!/usr/bin/env python3
"""
Run all Python tests for Rustka in order.
This script runs each test file and reports the results.
"""
import subprocess
import sys
import os
import time
from pathlib import Path

# Define test order - from basic to complex
TEST_ORDER = [
    # Basic connectivity and produce tests
    ("test_minimal.py", "Minimal connection test"),
    ("test_client.py", "Basic client test"),
    
    # API version and negotiation tests
    ("test_api_version.py", "API version compatibility"),
    ("test_api_negotiation.py", "API version negotiation test"),
    ("test_simple_negotiation.py", "Simple negotiation test"),
    
    # Fetch and throughput tests
    ("test_minimal_fetch.py", "Minimal fetch test"),
    ("test_modern_api.py", "Modern API throughput test"),
    
    # Record batch and advanced features
    ("test_recordbatch.py", "Record batch format test"),
    
    # Consumer group tests
    ("test_consumer_group.py", "Consumer group test"),
    
    # Compatibility test suites
    ("test_simple_compatibility.py", "Simple compatibility suite"),
    ("test_kafka_compatibility.py", "Full Kafka compatibility suite"),
    ("test_sentry_like.py", "Sentry-like usage patterns"),
    ("test_sentry_compatibility.py", "Sentry compatibility suite"),
    
    # Commit log tests
    ("test_commit_log_fix.py", "Commit log key fix verification"),
    ("test_snuba_compatibility.py", "Snuba commit log compatibility"),
]

def run_test(test_file, description):
    """Run a single test file and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {test_file}")
    print(f"Description: {description}")
    print('='*60)
    
    test_path = Path(__file__).parent / test_file
    
    if not test_path.exists():
        print(f"❌ Test file not found: {test_path}")
        return False
    
    try:
        # Use the Python interpreter from command line or default python3
        python_cmd = sys.executable if sys.executable else 'python3'
        
        result = subprocess.run(
            [python_cmd, str(test_path)],
            capture_output=True,
            text=True,
            timeout=60  # 60 second timeout per test
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print(f"\n✅ {test_file} PASSED")
            return True
        else:
            print(f"\n❌ {test_file} FAILED (exit code: {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n❌ {test_file} TIMEOUT (exceeded 60s)")
        return False
    except Exception as e:
        print(f"\n❌ {test_file} ERROR: {e}")
        return False

def main():
    """Run all tests and report summary."""
    print("="*60)
    print("RUSTKA TEST RUNNER")
    print("="*60)
    print(f"Running {len(TEST_ORDER)} tests...")
    print(f"Python: {sys.executable}")
    
    # Check if broker is running
    print("\n⚠️  Make sure the Rustka broker is running on localhost:9092")
    print("   Run 'cargo run --release' in another terminal")
    input("\nPress Enter to continue...")
    
    passed = 0
    failed = 0
    start_time = time.time()
    
    for test_file, description in TEST_ORDER:
        if run_test(test_file, description):
            passed += 1
        else:
            failed += 1
        
        # Small delay between tests
        time.sleep(0.5)
    
    elapsed = time.time() - start_time
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print('='*60)
    print(f"Total tests: {len(TEST_ORDER)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")
    print(f"Time: {elapsed:.1f}s")
    
    if failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {failed} tests failed")
        return 1

if __name__ == '__main__':
    sys.exit(main())
