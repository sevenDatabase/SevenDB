
import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import glob
import os

def main():
    files = glob.glob('throughput_vs_latency*.json')
    if not files:
        print("Error: No throughput_vs_latency*.json files found")
        sys.exit(1)

    plt.figure(figsize=(10, 6))

    for fname in files:
        try:
            with open(fname, 'r') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {fname}: {e}")
            continue

        df = pd.DataFrame(data)
        if df.empty:
            continue
            
        # Sort by actualOpsSec
        df = df.sort_values(by='actualOpsSec')

        # Label based on filename
        label = fname
        if "ratio" in fname:
            # Extract ratio
            parts = fname.replace(".json", "").split("_")
            try:
                label = f"Read Ratio {float(parts[-1]):.2f}"
            except:
                pass
        elif fname == "throughput_vs_latency.json":
            label = "Write Only (SET)"

        plt.plot(df['actualOpsSec'], df['latencyP99Ms'], marker='.', label=f'{label} P99')
        # We can also plot P50 if needed, but P99 is usually the focus for knee
        # plt.plot(df['actualOpsSec'], df['latencyP50Ms'], linestyle='--', alpha=0.5, label=f'{label} P50')

    plt.title('Throughput vs Latency (P99)')
    plt.xlabel('Throughput (ops/sec)')
    plt.ylabel('Latency (ms)')
    plt.grid(True)
    plt.legend()
    
    output_file = 'throughput_vs_latency_combined.png'
    plt.savefig(output_file)
    print(f"Plot saved to {output_file}")

if __name__ == "__main__":
    main()
