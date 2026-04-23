#!/usr/bin/env python3
"""
Parse YCSB multi-process test results and generate throughput vs time plot
Shows individual process throughput and aggregated throughput over time
"""

import re
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime
import os

def parse_multiprocess_log(filepath):
    """
    Parse YCSB multiprocess log file and extract time-series data for each process
    Returns: dict {process_id: [(time_sec, operations), ...]}
    """
    process_data = defaultdict(list)
    start_times = {}
    
    with open(filepath, 'r') as f:
        for line in f:
            # Extract process start time
            # Format: [Process X] Starting at HH:MM:SS.NNNNNNNNN
            start_match = re.search(r'\[Process (\d+)\] Starting at (\d{2}:\d{2}:\d{2}\.\d+)', line)
            if start_match:
                process_id = int(start_match.group(1))
                start_time_str = start_match.group(2)
                time_parts = start_time_str.split(':')
                hours = int(time_parts[0])
                minutes = int(time_parts[1])
                sec_parts = time_parts[2].split('.')
                seconds = int(sec_parts[0])
                microseconds = int(sec_parts[1]) if len(sec_parts) > 1 else 0
                start_times[process_id] = hours * 3600 + minutes * 60 + seconds + microseconds / 1e9
            
            # Extract status updates
            # Format: [Process X] YYYY-MM-DD HH:MM:SS N sec: OPERATIONS operations;
            status_match = re.search(r'\[Process (\d+)\] \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (\d+) sec: (\d+) operations', line)
            if status_match:
                process_id = int(status_match.group(1))
                time_sec = int(status_match.group(2))
                operations = int(status_match.group(3))
                process_data[process_id].append((time_sec, operations))
    
    return process_data, start_times

def parse_multithread_log(filepath):
    """
    Parse YCSB multithread log file and extract time-series data
    Multithread format has aggregated output from all threads in a single process
    Returns: dict {0: [(time_sec, operations), ...]} (single "process" with aggregated data)
    """
    process_data = defaultdict(list)
    start_times = {}
    
    with open(filepath, 'r') as f:
        for line in f:
            # Extract status updates (no process prefix)
            # Format: YYYY-MM-DD HH:MM:SS N sec: OPERATIONS operations;
            status_match = re.search(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (\d+) sec: (\d+) operations', line)
            if status_match:
                # Use process_id 0 for aggregated multithread data
                process_id = 0
                time_sec = int(status_match.group(1))
                operations = int(status_match.group(2))
                process_data[process_id].append((time_sec, operations))
    
    return process_data, start_times

def parse_log_file(filepath):
    """
    Parse YCSB log file and extract time-series data
    Automatically detects multiprocess vs multithread format
    Returns: (process_data, start_times, test_type)
    """
    test_type = detect_test_type(filepath)
    
    if test_type == 'multiprocess':
        process_data, start_times = parse_multiprocess_log(filepath)
    else:
        process_data, start_times = parse_multithread_log(filepath)
    
    return process_data, start_times, test_type

def calculate_instantaneous_throughput(process_data):
    """
    Calculate instantaneous throughput for each process
    Returns: dict {process_id: [(time_sec, throughput_ops_per_sec), ...]}
    """
    throughput_data = {}
    
    for process_id, data_points in process_data.items():
        throughput_points = []
        
        # First point: throughput is 0 (or operations / time if time > 0)
        if len(data_points) > 0:
            first_time, first_ops = data_points[0]
            if first_time == 0:
                throughput_points.append((0, 0))
            else:
                throughput_points.append((first_time, first_ops / first_time))
        
        for i in range(1, len(data_points)):
            prev_time, prev_ops = data_points[i-1]
            curr_time, curr_ops = data_points[i]
            
            if curr_time > prev_time:
                delta_time = curr_time - prev_time
                delta_ops = curr_ops - prev_ops
                throughput = delta_ops / delta_time if delta_time > 0 else 0
                throughput_points.append((curr_time, throughput))
            else:
                # Same time point, use cumulative throughput
                throughput = curr_ops / curr_time if curr_time > 0 else 0
                throughput_points.append((curr_time, throughput))
        
        throughput_data[process_id] = throughput_points
    
    return throughput_data

def calculate_aggregated_throughput(throughput_data):
    """
    Calculate aggregated throughput at each time point
    Returns: [(time_sec, aggregated_throughput), ...]
    """
    # Collect all time points
    all_times = set()
    for process_data in throughput_data.values():
        for time_sec, _ in process_data:
            all_times.add(time_sec)
    
    all_times = sorted(all_times)
    
    # For each time point, sum throughputs from all processes
    aggregated = []
    for time_sec in all_times:
        total_throughput = 0
        for process_data in throughput_data.values():
            # Find the throughput for this process at this time (or closest previous)
            process_throughput = 0
            for t, tp in process_data:
                if t <= time_sec:
                    process_throughput = tp
                else:
                    break
            total_throughput += process_throughput
        
        aggregated.append((time_sec, total_throughput))
    
    return aggregated

def extract_aggregated_throughput(filepath):
    """
    Extract the aggregated throughput value from the log file
    Format: "Aggregated throughput: 70655.14 ops/sec"
    Returns: float or None if not found
    """
    with open(filepath, 'r') as f:
        for line in f:
            match = re.search(r'Aggregated throughput:\s*([\d.]+)\s*ops/sec', line)
            if match:
                return float(match.group(1))
    return None

def plot_multiprocess_throughput_time(filepath):
    """
    Plot throughput vs time for multiprocess results (original method, unchanged)
    """
    process_data, start_times = parse_multiprocess_log(filepath)
    
    if not process_data:
        print(f"Error: No process data found in {filepath}")
        return
    
    throughput_data = calculate_instantaneous_throughput(process_data)
    aggregated_data = calculate_aggregated_throughput(throughput_data)
    
    # Extract average aggregated throughput from file
    avg_aggregated_throughput = extract_aggregated_throughput(filepath)
    
    plt.figure(figsize=(12, 7))
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    for i, (process_id, data_points) in enumerate(sorted(throughput_data.items())):
        times = [t for t, _ in data_points]
        throughputs = [tp for _, tp in data_points]
        color = colors[i % len(colors)]
        plt.plot(times, throughputs, marker='o', markersize=4, linewidth=2, 
                label=f'Process {process_id}', color=color, alpha=0.7)
    
    # Plot aggregated throughput
    agg_times = [t for t, _ in aggregated_data]
    agg_throughputs = [tp for _, tp in aggregated_data]
    plt.plot(agg_times, agg_throughputs, marker='s', markersize=6, linewidth=3, 
            label='Aggregated (instantaneous)', color='black', linestyle='--', alpha=0.9)
    
    # Plot average aggregated throughput as horizontal line
    if avg_aggregated_throughput is not None:
        max_time = max(agg_times) if agg_times else 0
        plt.axhline(y=avg_aggregated_throughput, color='red', linestyle='-', linewidth=2, 
                   label=f'Aggregated (average): {avg_aggregated_throughput:.2f} ops/sec', alpha=0.8)
    
    plt.xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    plt.ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    
    filename = filepath.split('/')[-1]
    title = f'Throughput vs Time (Multiprocess): {filename}'
    plt.title(title, fontsize=14, fontweight='bold')
    
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend(loc='best', fontsize=10)
    plt.tight_layout()
    
    if output_file is None:
        output_file = filepath.replace('.txt', '_multiprocess_throughput_time.png')
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    plt.show()

def plot_multithread_throughput_time(filepath):
    """
    Plot throughput vs time for multithread results
    Shows aggregated throughput from all threads (single line)
    For multithread, there's only 1 process (aggregated from all threads),
    so throughput_data and aggregated_data are the same
    """
    process_data, start_times = parse_multithread_log(filepath)
    
    if not process_data:
        print(f"Error: No process data found in {filepath}")
        return
    
    throughput_data = calculate_instantaneous_throughput(process_data)
    
    # For multithread, there's only one process (process_id=0), so throughput_data is already aggregated
    # No need to call calculate_aggregated_throughput since there's only one process
    if 0 not in throughput_data:
        print(f"Error: Expected process_id=0 in multithread data")
        return
    
    # Extract average aggregated throughput from file
    avg_aggregated_throughput = extract_aggregated_throughput(filepath)
    
    plt.figure(figsize=(12, 7))
    
    times = [t for t, _ in throughput_data[0]]
    throughputs = [tp for _, tp in throughput_data[0]]
    plt.plot(times, throughputs, marker='o', markersize=5, linewidth=3, 
            label='Aggregated Throughput (all threads)', color='#1f77b4', alpha=0.8)
    
    if avg_aggregated_throughput is not None:
        max_time = max(times) if times else 0
        plt.axhline(y=avg_aggregated_throughput, color='red', linestyle='--', linewidth=2, 
                   label=f'Average: {avg_aggregated_throughput:.2f} ops/sec', alpha=0.8)
    
    plt.xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    plt.ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    
    filename = filepath.split('/')[-1]
    title = f'Throughput vs Time (Multithread): {filename}'
    plt.title(title, fontsize=14, fontweight='bold')
    
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend(loc='best', fontsize=10)
    plt.tight_layout()
    
    output_file = filepath.replace('.txt', '_multithread_throughput_time.png')
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    plt.show()

def plot_throughput_time(filepath, test_type):
    if test_type == 'multiprocess':
        plot_multiprocess_throughput_time(filepath)
    else:
        plot_multithread_throughput_time(filepath)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot_throughput_time.py <log_file> [output_file]")
        print("Example: python3 plot_throughput_time.py multiple-multithread-result")
        sys.exit(1)
    
    result_dir = sys.argv[1]
    dir_path = os.path.dirname(os.path.abspath(result_dir))
    dir_name = os.path.basename(dir_path)
    
    test_type = None
    if 'multiprocess' in dir_name.lower():
        test_type = 'multiprocess'
    elif 'multithread' in dir_name.lower():
        test_type = 'multithread'
    
    for file in os.listdir(result_dir):
        if file.endswith('.txt'):
            filepath = os.path.join(result_dir, file)
            plot_throughput_time(filepath,test_type)

if __name__ == '__main__':
    main()
