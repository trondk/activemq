#!/usr/bin/env python3
"""
Extract network statistics from strace log file.
Analyzes socket operations, read/write calls, and network performance.
"""

import re
import sys
from collections import defaultdict
from datetime import datetime

def parse_timestamp(ts_str):
    """Parse timestamp from strace output (HH:MM:SS.microseconds)"""
    try:
        h, m, s = ts_str.split(':')
        sec, usec = s.split('.')
        total_sec = int(h) * 3600 + int(m) * 60 + int(sec)
        total_usec = total_sec * 1000000 + int(usec)
        return total_usec
    except:
        return 0

def main():
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    else:
        filename = 'trace.log'

    # Statistics tracking
    stats = {
        'socket_calls': 0,
        'connect_calls': 0,
        'read_calls': 0,
        'write_calls': 0,
        'bytes_read': 0,
        'bytes_written': 0,
        'failed_reads': 0,
        'failed_writes': 0,
        'eagain_count': 0,
        'connections': [],
    }

    fd_stats = defaultdict(lambda: {'reads': 0, 'writes': 0, 'bytes_read': 0, 'bytes_written': 0})

    # Regex patterns
    socket_pattern = re.compile(r'socket\(')
    connect_pattern = re.compile(r'connect\((\d+),.*sin_addr=inet_addr\("([^"]+)"\).*sin_port=htons\((\d+)\)')
    read_pattern = re.compile(r'read\((\d+),.*\)\s*=\s*(-?\d+)')
    write_pattern = re.compile(r'write\((\d+),.*\)\s*=\s*(-?\d+)')
    eagain_pattern = re.compile(r'EAGAIN')
    timestamp_pattern = re.compile(r'^\s*\d+→?\d*\s+(\d+:\d+:\d+\.\d+)')

    first_ts = None
    last_ts = None

    print(f"Analyzing {filename}...")
    print("=" * 80)

    with open(filename, 'r') as f:
        for line in f:
            # Extract timestamp
            ts_match = timestamp_pattern.search(line)
            if ts_match:
                ts = parse_timestamp(ts_match.group(1))
                if first_ts is None:
                    first_ts = ts
                last_ts = ts

            # Socket creation
            if socket_pattern.search(line):
                stats['socket_calls'] += 1

            # Connect calls
            connect_match = connect_pattern.search(line)
            if connect_match:
                stats['connect_calls'] += 1
                fd = connect_match.group(1)
                ip = connect_match.group(2)
                port = connect_match.group(3)
                stats['connections'].append({'fd': fd, 'ip': ip, 'port': port})

            # Read calls
            read_match = read_pattern.search(line)
            if read_match:
                fd = int(read_match.group(1))
                bytes_count = int(read_match.group(2))
                stats['read_calls'] += 1
                if bytes_count > 0:
                    stats['bytes_read'] += bytes_count
                    fd_stats[fd]['reads'] += 1
                    fd_stats[fd]['bytes_read'] += bytes_count
                elif bytes_count == -1:
                    stats['failed_reads'] += 1

            # Write calls
            write_match = write_pattern.search(line)
            if write_match:
                fd = int(write_match.group(1))
                bytes_count = int(write_match.group(2))
                stats['write_calls'] += 1
                if bytes_count > 0:
                    stats['bytes_written'] += bytes_count
                    fd_stats[fd]['writes'] += 1
                    fd_stats[fd]['bytes_written'] += bytes_count
                elif bytes_count == -1:
                    stats['failed_writes'] += 1

            # EAGAIN errors
            if eagain_pattern.search(line):
                stats['eagain_count'] += 1

    # Calculate duration
    if first_ts and last_ts:
        duration_sec = (last_ts - first_ts) / 1000000.0
    else:
        duration_sec = 0

    # Print summary
    print("\n📊 NETWORK STATISTICS SUMMARY")
    print("=" * 80)
    print(f"Duration: {duration_sec:.3f} seconds")
    print()

    print("🔌 Connection Statistics:")
    print(f"  Socket calls:        {stats['socket_calls']}")
    print(f"  Connect calls:       {stats['connect_calls']}")
    print(f"  Unique connections:  {len(stats['connections'])}")
    for conn in stats['connections']:
        print(f"    → {conn['ip']}:{conn['port']} (fd={conn['fd']})")
    print()

    print("📥 Read Statistics:")
    print(f"  Total read calls:    {stats['read_calls']}")
    print(f"  Successful reads:    {stats['read_calls'] - stats['failed_reads']}")
    print(f"  Failed reads:        {stats['failed_reads']}")
    print(f"  Total bytes read:    {stats['bytes_read']:,} bytes ({stats['bytes_read'] / 1024:.2f} KB)")
    if duration_sec > 0:
        print(f"  Read throughput:     {stats['bytes_read'] / duration_sec / 1024:.2f} KB/s")
    print()

    print("📤 Write Statistics:")
    print(f"  Total write calls:   {stats['write_calls']}")
    print(f"  Successful writes:   {stats['write_calls'] - stats['failed_writes']}")
    print(f"  Failed writes:       {stats['failed_writes']}")
    print(f"  Total bytes written: {stats['bytes_written']:,} bytes ({stats['bytes_written'] / 1024:.2f} KB)")
    if duration_sec > 0:
        print(f"  Write throughput:    {stats['bytes_written'] / duration_sec / 1024:.2f} KB/s")
    print()

    print("⚠️  Error Statistics:")
    print(f"  EAGAIN errors:       {stats['eagain_count']}")
    print()

    # Per-FD statistics (filter network FDs, typically >= 3)
    network_fds = {fd: data for fd, data in fd_stats.items() if fd >= 3 and (data['reads'] > 0 or data['writes'] > 0)}
    if network_fds:
        print("📊 Per File Descriptor Statistics (FD >= 3):")
        print(f"  {'FD':<6} {'Reads':<10} {'Writes':<10} {'Bytes Read':<15} {'Bytes Written':<15}")
        print("  " + "-" * 70)
        for fd in sorted(network_fds.keys()):
            data = network_fds[fd]
            print(f"  {fd:<6} {data['reads']:<10} {data['writes']:<10} {data['bytes_read']:<15,} {data['bytes_written']:<15,}")

    print("=" * 80)

if __name__ == '__main__':
    main()
