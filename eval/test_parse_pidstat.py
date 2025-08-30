#!/usr/bin/env python3
"""
Test script to verify pidstat parser extracts correct values.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'eval'))

from parse_pidstat import parse_pidstat_file
import statistics

def test_pidstat_parser():
    """Test the pidstat parser with known test data."""

    # Expected values from our test data
    expected_cpu_values = [15.00, 5.00, 1.00, 18.00, 6.00]  # %CPU values
    expected_rss_values = [500000, 750000, 100000, 550000, 800000]  # RSS values in KB
    expected_mem_percent_values = [1.50, 2.25, 0.30, 1.65, 2.40]  # %MEM values

    # Parse the test file
    test_file = "test_pidstat_data.txt"
    cpu_data, memory_data = parse_pidstat_file(test_file)

    # Extract actual values
    actual_rss_values = [m['rss_kb'] for m in memory_data]
    actual_mem_percent_values = [m['mem_percent'] for m in memory_data]

    print("=== Test Results ===")
    print(f"Expected CPU values: {expected_cpu_values}")
    print(f"Actual CPU values: {cpu_data}")
    print(f"CPU values match: {cpu_data == expected_cpu_values}")
    print()

    print(f"Expected RSS values: {expected_rss_values}")
    print(f"Actual RSS values: {actual_rss_values}")
    print(f"RSS values match: {actual_rss_values == expected_rss_values}")
    print()

    print(f"Expected %MEM values: {expected_mem_percent_values}")
    print(f"Actual %MEM values: {actual_mem_percent_values}")
    print(f"%MEM values match: {actual_mem_percent_values == expected_mem_percent_values}")
    print()

    # Test statistics calculations
    if cpu_data:
        print("=== CPU Statistics ===")
        print(f"Count: {len(cpu_data)}")
        print(f"Min: {min(cpu_data):.2f}")
        print(f"Max: {max(cpu_data):.2f}")
        print(f"Mean: {statistics.mean(cpu_data):.2f}")
        print(f"Expected Mean: {statistics.mean(expected_cpu_values):.2f}")
        print()

    if actual_rss_values:
        print("=== Memory RSS Statistics ===")
        print(f"Count: {len(actual_rss_values)}")
        print(f"Min: {min(actual_rss_values):.2f}")
        print(f"Max: {max(actual_rss_values):.2f}")
        print(f"Mean: {statistics.mean(actual_rss_values):.2f}")
        print(f"Expected Mean: {statistics.mean(expected_rss_values):.2f}")
        print()

    # Overall test result
    all_tests_pass = (
        cpu_data == expected_cpu_values and
        actual_rss_values == expected_rss_values and
        actual_mem_percent_values == expected_mem_percent_values
    )

    print("=== Overall Test Result ===")
    if all_tests_pass:
        print("✓ ALL TESTS PASSED - Parser is working correctly!")
        return True
    else:
        print("✗ SOME TESTS FAILED - Parser needs fixing!")
        return False

if __name__ == "__main__":
    success = test_pidstat_parser()
    sys.exit(0 if success else 1)
