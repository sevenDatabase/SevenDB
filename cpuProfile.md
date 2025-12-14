Throughput vs Latency Benchmark Results
We have enhanced the benchmark to include mixed workloads and CPU profiling.

Benchmark Configuration
Conns: 50
Payload: 64 bytes
Step Duration: 5s
Workloads:
Write Only: 100% SET (Ratio 0.0)
Read Heavy: 90% GET (Ratio 0.9)
Mixed: 50% GET (Ratio 0.5)
Results
Throughput vs Latency Curve
The following plot shows how latency increases as throughput rises for different workloads. The "knee" is the point where latency starts spiking non-linearly.

Throughput vs Latency

Explaining the Knee: CPU Profile
We captured a CPU profile at ~30k ops/sec (near the knee for some workloads).

Flamegraph

Profile Analysis (Top Consumers):

runtime.futex (~15%): Indicates significant lock contention or synchronization overhead.
internal.(*TCPWire).Receive (~14%): Time spent reading responses from the server.
syscall.Read / runtime.netpoll: Network I/O.
This suggests the client is heavily I/O bound and spending time coordinating goroutines. The "knee" likely appears when the server's processing time increases slightly, causing backpressure that increases the time clients wait in Receive and futex (waiting for connection/channel slots).

Verification
Mixed Workloads: Successfully ran 0%, 50%, and 90% read ratios.
Profiling: Successfully captured 
cpu_profile_30k.pprof
 and generated flamegraph.
Plotting: Generated combined plot showing all workloads.
