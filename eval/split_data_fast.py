#!/usr/bin/env python3
"""
Ultra-fast single-pass data splitter with optimized I/O buffering.
Uses regex extraction (no JSON parsing) with large write buffers.
Optimized for 160GB+ files - should reach disk bandwidth limits.
"""

import re
import sys
from pathlib import Path

# Compiled regex for fast metric name extraction
METRIC_NAME_PATTERN = re.compile(rb'"name"\s*:\s*"([^"]+)"')

# Large buffer size for write operations (4MB per file)
WRITE_BUFFER_SIZE = 4 * 1024 * 1024


def process_json_file(json_file_path, output_dir, show_progress=True):
    """Process a single JSON file with optimized buffering."""
    json_filename = json_file_path.stem
    output_subdir = output_dir / json_filename
    output_subdir.mkdir(parents=True, exist_ok=True)

    # Dictionary to store open file handles with large buffers
    metric_files = {}

    # Track progress
    line_count = 0
    progress_interval = 5000000  # Report every 5 million lines

    print(f"  → Extracting all metrics in single pass...")

    try:
        # Open source file in binary mode with large read buffer
        with open(json_file_path, 'rb', buffering=8*1024*1024) as f:
            for line in f:
                line_count += 1

                # Extract metric name using regex (no JSON parsing)
                match = METRIC_NAME_PATTERN.search(line)
                if match:
                    metric_name = match.group(1).decode('utf-8')

                    # Open file handle if not already open (with large buffer)
                    if metric_name not in metric_files:
                        output_file = output_subdir / f"{metric_name}.json"
                        metric_files[metric_name] = open(
                            output_file, 'wb', buffering=WRITE_BUFFER_SIZE
                        )

                    # Write line directly (already in bytes)
                    metric_files[metric_name].write(line)

                # Print progress
                if show_progress and line_count % progress_interval == 0:
                    print(f"    Processed {line_count:,} lines...", file=sys.stderr, flush=True)

    finally:
        # Close all file handles (flushes buffers)
        for file_handle in metric_files.values():
            file_handle.close()

    if show_progress and line_count > 0:
        print(f"    Completed {line_count:,} total lines", file=sys.stderr, flush=True)

    # Report results
    metric_count = len(metric_files)
    print(f"  ✓ Extracted {metric_count} unique metrics")

    return line_count, metric_files.keys()


def validate_output(json_file_path, output_dir):
    """Validate that all lines were extracted."""
    json_filename = json_file_path.stem
    output_subdir = output_dir / json_filename

    print()
    print("  Validation:")

    # Count source lines
    print("  → Counting source file lines...")
    with open(json_file_path, 'rb') as f:
        source_lines = sum(1 for _ in f)
    print(f"    Source lines: {source_lines:,}")

    # Count extracted lines
    print("  → Counting extracted lines in split_output...")
    extracted_lines = 0
    for metric_file in output_subdir.glob("*.json"):
        with open(metric_file, 'rb') as f:
            extracted_lines += sum(1 for _ in f)
    print(f"    Extracted lines: {extracted_lines:,}")

    # Compare
    if source_lines == extracted_lines:
        print("    Status: ✓ PASS - All lines accounted for")
        return True
    else:
        missing = source_lines - extracted_lines
        print(f"    Status: ✗ FAIL - Missing {missing:,} lines!")
        print("    Possible causes:")
        print("      - Lines with missing 'name' field")
        print("      - Empty or malformed lines")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Ultra-fast data splitter with optimized I/O buffering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data/BLOCK-PROMO                    # Fast mode (no validation)
  %(prog)s --validate data/BLOCK-PROMO         # Extract + validate
  %(prog)s --validate-only data/BLOCK-PROMO    # Only validate existing output

Uses regex extraction (no JSON parsing) with 4MB write buffers per file.
        """
    )
    parser.add_argument(
        'target_directory',
        type=Path,
        help='Directory containing JSON files to split'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Enable validation (compares source vs extracted line counts)'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate existing split_output, don\'t re-extract'
    )

    args = parser.parse_args()

    target_dir = args.target_directory
    output_dir = target_dir / "split_output"

    # Validate directory
    if not target_dir.exists():
        print(f"Error: Directory {target_dir} does not exist")
        sys.exit(1)

    if not target_dir.is_dir():
        print(f"Error: {target_dir} is not a directory")
        sys.exit(1)

    # Find all JSON files
    json_files = list(target_dir.glob("*.json"))

    if not json_files:
        print(f"No JSON files found in {target_dir}")
        sys.exit(1)

    # Header
    if args.validate_only:
        print("=== Data Validation (validate-only mode) ===")
    else:
        print("=== Fast Data Splitter (Python optimized I/O) ===")

    print(f"Found {len(json_files)} JSON files to process")
    if not args.validate_only:
        print("Processing: SINGLE-PASS (reads each file once)")
        print(f"Write buffer: {WRITE_BUFFER_SIZE // (1024*1024)}MB per file")

    if args.validate or args.validate_only:
        print("Validation: ENABLED")
    else:
        print("Validation: DISABLED (use --validate to enable)")
    print()

    # Process files
    validation_passed = 0
    validation_failed = 0

    for json_file in json_files:
        print(f"Processing {json_file.name}...")

        # Show file size
        file_size = json_file.stat().st_size
        file_size_gb = file_size / (1024 ** 3)
        print(f"  File size: {file_size_gb:.2f} GB")

        # Extract (unless validate-only)
        if not args.validate_only:
            line_count, metrics = process_json_file(
                json_file, output_dir, show_progress=not args.validate
            )

        # Validate if requested
        if args.validate or args.validate_only:
            if validate_output(json_file, output_dir):
                validation_passed += 1
            else:
                validation_failed += 1

        print(f"  ✓ Completed {json_file.name}")
        print()

    # Summary
    if args.validate_only:
        print("✓ Validation complete!")
    else:
        print(f"✓ Data splitting complete! Output saved to {output_dir}")

    print()
    print("═" * 63)
    print("Summary:")
    print(f"  Source: {target_dir}")
    print(f"  Output: {output_dir}")
    print(f"  Files processed: {len(json_files)}")
    print()

    # Validation summary
    if args.validate or args.validate_only:
        print("Validation Results:")
        print(f"  ✓ Passed: {validation_passed} files")
        print(f"  ✗ Failed: {validation_failed} files")
        print()

        if validation_failed > 0:
            print("⚠ WARNING: Some files had missing lines!")
            print("Check the validation output above for details.")
            sys.exit(1)
        else:
            print("✓ All files validated successfully - no data loss detected")


if __name__ == "__main__":
    main()
