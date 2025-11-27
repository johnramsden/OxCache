#!/usr/bin/env python3
"""
Consolidate Metrics Script

This script consolidates log files from a hierarchical directory structure
into a flat consolidated directory. Specifically designed for zns-param-logs structure.

Usage:
    python3 consolidate_metrics.py <input_directory> [--dry-run]

Example:
    python3 consolidate_metrics.py data/logs/zns-param-logs
    python3 consolidate_metrics.py data/logs/zns-param-logs --dry-run
"""

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import List, Tuple, Dict


class ConsolidationStats:
    """Track statistics for the consolidation process"""
    def __init__(self):
        self.dirs_processed = 0
        self.files_moved = 0
        self.duplicates_skipped = 0
        self.errors = []
        self.moved_files = []
        self.skipped_files = []


def find_logs_directories(input_dir: Path) -> List[Path]:
    """
    Find all subdirectories that contain a 'logs' subdirectory.

    Args:
        input_dir: Root directory to search

    Returns:
        List of paths to 'logs' subdirectories
    """
    logs_dirs = []

    # Walk through the input directory
    for item in input_dir.iterdir():
        if item.is_dir():
            logs_path = item / "logs"
            if logs_path.exists() and logs_path.is_dir():
                logs_dirs.append(logs_path)

    return sorted(logs_dirs)


def get_json_file(logs_dir: Path) -> Path | None:
    """
    Find the metrics-*.json file in the logs directory.

    Args:
        logs_dir: Path to logs directory

    Returns:
        Path to JSON file or None if not found
    """
    json_files = list(logs_dir.glob("metrics-*.json"))

    if len(json_files) == 0:
        return None
    elif len(json_files) > 1:
        # If multiple JSON files, take the first one and warn
        print(f"  ⚠️  Warning: Multiple JSON files found in {logs_dir}, using {json_files[0].name}")

    return json_files[0]


def process_logs_directory(
    logs_dir: Path,
    consolidated_dir: Path,
    stats: ConsolidationStats,
    dry_run: bool = False
) -> bool:
    """
    Process a single logs directory: rename JSON file and move all log files.

    Args:
        logs_dir: Path to logs directory
        consolidated_dir: Path to consolidated output directory
        stats: Statistics tracker
        dry_run: If True, only simulate operations

    Returns:
        True if successful, False otherwise
    """
    parent_dir = logs_dir.parent
    parent_name = parent_dir.name

    print(f"\nProcessing: {parent_name}")

    # Find the JSON metrics file
    json_file = get_json_file(logs_dir)
    if json_file is None:
        error_msg = f"  ❌ No metrics-*.json file found in {logs_dir}"
        print(error_msg)
        stats.errors.append(error_msg)
        return False

    # Define the new JSON filename
    new_json_name = f"{parent_name}.json"
    new_json_path = consolidated_dir / new_json_name

    # Check for duplicate
    if new_json_path.exists():
        warning_msg = f"  ⚠️  Skipping duplicate: {new_json_name} already exists"
        print(warning_msg)
        stats.duplicates_skipped += 1
        stats.skipped_files.append(new_json_name)
        return False

    # Find all files to move (excluding .keep)
    files_to_move = []

    # Add renamed JSON file
    files_to_move.append((json_file, new_json_path))

    # Add other log files (.client, .server, .pidstat)
    for pattern in ["*.client", "*.server", "*.pidstat"]:
        matching_files = list(logs_dir.glob(pattern))
        for file in matching_files:
            dest_path = consolidated_dir / file.name
            files_to_move.append((file, dest_path))

    # Move files
    for src, dest in files_to_move:
        try:
            if dry_run:
                print(f"  [DRY RUN] Would move: {src.name} -> {dest.name}")
            else:
                shutil.move(str(src), str(dest))
                print(f"  ✓ Moved: {src.name} -> {dest.name}")

            stats.files_moved += 1
            stats.moved_files.append(dest.name)
        except Exception as e:
            error_msg = f"  ❌ Error moving {src.name}: {e}"
            print(error_msg)
            stats.errors.append(error_msg)
            return False

    stats.dirs_processed += 1
    return True


def cleanup_parent_directory(parent_dir: Path, dry_run: bool = False) -> bool:
    """
    Remove the parent directory after successful processing.

    Args:
        parent_dir: Directory to remove
        dry_run: If True, only simulate operations

    Returns:
        True if successful, False otherwise
    """
    try:
        if dry_run:
            print(f"  [DRY RUN] Would remove directory: {parent_dir}")
        else:
            shutil.rmtree(parent_dir)
            print(f"  🗑️  Removed directory: {parent_dir.name}")
        return True
    except Exception as e:
        print(f"  ❌ Error removing {parent_dir}: {e}")
        return False


def print_summary(stats: ConsolidationStats):
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("CONSOLIDATION SUMMARY")
    print("=" * 60)
    print(f"Directories processed: {stats.dirs_processed}")
    print(f"Files moved: {stats.files_moved}")
    print(f"Duplicates skipped: {stats.duplicates_skipped}")
    print(f"Errors encountered: {len(stats.errors)}")

    if stats.errors:
        print("\nErrors:")
        for error in stats.errors:
            print(f"  - {error}")

    if stats.skipped_files:
        print("\nSkipped files (duplicates):")
        for skipped in stats.skipped_files:
            print(f"  - {skipped}")

    print("=" * 60)


def main():
    """Main entry point for the consolidation script"""
    parser = argparse.ArgumentParser(
        description="Consolidate metrics log files into a flat directory structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python3 consolidate_metrics.py data/logs/zns-param-logs
  python3 consolidate_metrics.py data/logs/zns-param-logs --dry-run
        """
    )
    parser.add_argument(
        "input_directory",
        help="Input directory containing subdirectories with logs folders"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate operations without making actual changes"
    )

    args = parser.parse_args()

    # Validate input directory
    input_dir = Path(args.input_directory)
    if not input_dir.exists():
        print(f"❌ Error: Input directory '{input_dir}' does not exist")
        sys.exit(1)

    if not input_dir.is_dir():
        print(f"❌ Error: '{input_dir}' is not a directory")
        sys.exit(1)

    # Create consolidated directory name
    consolidated_dir_name = f"{input_dir.name}-consolidated"
    consolidated_dir = input_dir.parent / consolidated_dir_name

    # Check if consolidated directory already exists
    if consolidated_dir.exists():
        response = input(f"⚠️  Directory '{consolidated_dir}' already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Aborted.")
            sys.exit(0)
        else:
            if not args.dry_run:
                shutil.rmtree(consolidated_dir)

    # Create consolidated directory
    if args.dry_run:
        print(f"[DRY RUN] Would create directory: {consolidated_dir}")
    else:
        consolidated_dir.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created consolidated directory: {consolidated_dir}")

    # Find all logs directories
    print(f"\nScanning for logs directories in: {input_dir}")
    logs_dirs = find_logs_directories(input_dir)
    print(f"Found {len(logs_dirs)} logs directories to process")

    if len(logs_dirs) == 0:
        print("No logs directories found. Exiting.")
        sys.exit(0)

    # Process each logs directory
    stats = ConsolidationStats()
    dirs_to_cleanup = []

    for logs_dir in logs_dirs:
        success = process_logs_directory(logs_dir, consolidated_dir, stats, args.dry_run)
        if success:
            dirs_to_cleanup.append(logs_dir.parent)

    # Cleanup parent directories
    if dirs_to_cleanup:
        print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Cleaning up processed directories...")
        for parent_dir in dirs_to_cleanup:
            cleanup_parent_directory(parent_dir, args.dry_run)

    # Print summary
    print_summary(stats)

    if args.dry_run:
        print("\n💡 This was a dry run. No actual changes were made.")
        print("   Run without --dry-run to perform the consolidation.")


if __name__ == "__main__":
    main()
