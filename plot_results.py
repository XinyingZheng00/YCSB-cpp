#!/usr/bin/env python3
"""
Parse YCSB multi-process test results and generate throughput comparison plot
"""

import re
import os
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import random
import sys

def extract_throughput(filepath):
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            # Look for "Aggregated throughput: XXXX.XX ops/sec"
            match = re.search(r'Aggregated throughput:\s*([\d.]+)\s*ops/sec', content)
            if match:
                return float(match.group(1))
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return None

def parse_results(result_dir):
    result_dir = Path(result_dir)
    data = {}
    
    for filename in result_dir.glob('*.txt'):
        db_name = filename.stem.split('-')[0]
        page_size = filename.stem.split('-')[1]
        db_type = f"{db_name}-{page_size}"
        process_count = int(filename.stem.split('-')[2])
        throughput = extract_throughput(filename)
        if throughput is not None:
            if db_type not in data:
                data[db_type] = {}
            data[db_type][process_count] = throughput
        else:
            print(f"Warning: Could not extract throughput from {filename}")
    
    return data

def plot_results(data, output_file='throughput_comparison.png'):
    db_types = list(data.keys())
    db_labels = {db_type: db_type for db_type in db_types}
    db_types_sorted = sorted(db_types)
    colors = {db_type: '#' + ''.join([f'{random.randint(0, 255):02x}' for _ in range(3)]) for db_type in db_types}
    process_counts = sorted(set().union(*[db_data.keys() for db_data in data.values()]))
    
    plt.figure(figsize=(10, 6))
    for db in db_types_sorted:
        plt.plot(process_counts, [data[db][process_count] for process_count in process_counts], 
                 marker='o', 
                 linewidth=2, 
                 markersize=8,
                 label=db_labels[db],
                 color=colors[db])
    
    plt.xlabel('Number of Writers', fontsize=12, fontweight='bold')
    plt.ylabel('Aggregated Throughput (ops/sec)', fontsize=12, fontweight='bold')
    plt.title('Multi-Writer Write Performance Comparison', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend(loc='best', fontsize=10)
    plt.xticks(process_counts)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    plt.show()

def main():
    
    result_dir = 'multiple-multithread-result'
    if len(sys.argv) > 1:
        result_dir = sys.argv[1]
    
    if not os.path.exists(result_dir):
        print(f"Error: Directory '{result_dir}' not found")
        sys.exit(1)
    
    print(f"Parsing results from: {result_dir}")
    data = parse_results(result_dir)
    output_file = result_dir + '/throughput_comparison.png'
    if len(sys.argv) > 2:
        output_file = result_dir + '/' + sys.argv[2]
    
    plot_results(data, output_file)

if __name__ == '__main__':
    main()

