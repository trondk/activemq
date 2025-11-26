# ActiveMQ AMQP Tools

High-performance AMQP 1.0 producer and consumer tools written in Go for Apache ActiveMQ Artemis benchmarking and testing.

## Overview

This project provides command-line tools for working with ActiveMQ Artemis via the AMQP 1.0 protocol:

- **Producer** (pack.ag/amqp): Original producer with basic AMQP support
- **ProducerV2** (Azure AMQP): High-performance producer with smart settlement modes (13x faster for non-durable)
- **Consumer**: Receives and acknowledges messages with performance metrics
- **DiskTest**: Disk I/O benchmark tool to demonstrate durable vs non-durable performance differences

### Key Features:
- Real-time throughput reporting with thread count display
- Concurrent producer threads for maximum throughput
- Smart settlement modes (Settled for non-durable, Unsettled for durable)
- Graceful shutdown with final statistics
- SASL PLAIN authentication
- Cluster support with failover
- Message file loading with header support
- Atomic counters for accurate metrics

## Prerequisites

- Go 1.19 or higher
- Apache ActiveMQ Artemis with AMQP acceptor enabled (default port 5672)

## Installation

Clone and build all tools:

```bash
git clone https://github.com/trondk/activemq.git
cd activemq

# Build all tools
go build -o producer producer.go
go build -o producerv2 producerv2.go
go build -o consumer consumer.go
go build -o disktest disktest.go
```

## Producer Usage

### Command Line Options

```bash
./producer [OPTIONS]

Options:
  -server string
        The host and port of the ActiveMQ Artemis AMQP acceptor (required)
  -size int
        The size of the payload for each message sent (default: 1024)
  -username string
        Username for authentication (default: "admin")
  -password string
        Password for authentication (default: "admin")
  -queue string
        The target queue where messages will be sent (default: "DLQ")
```

### Producer Examples

**Send 1KB messages:**
```bash
./producer -server localhost:5672
```

**Send 10KB messages to custom queue:**
```bash
./producer -server localhost:5672 -size 10240 -queue testqueue
```

**Custom credentials:**
```bash
./producer -server localhost:5672 -username myuser -password mypass
```

## ProducerV2 Usage (Recommended)

ProducerV2 uses the Azure AMQP library with intelligent settlement modes for significantly better performance.

### Command Line Options

```bash
./producerv2 [OPTIONS]

Options:
  -server string
        Comma-separated list of host:port of the ActiveMQ Artemis AMQP acceptors
  -size int
        The size of the payload for each message sent (default: 1024)
  -username string
        Username for authentication (default: "admin")
  -password string
        Password for authentication (default: "admin")
  -queue string
        The target queue where messages will be sent (default: "DLQ")
  -durable
        Set message delivery mode to Persistent (Durable). Uses SenderSettleModeUnsettled
        for reliability, SenderSettleModeSettled (fire-and-forget) for non-durable.
  -file string
        Optional: Load message content from a text file instead of generating dummy payload
  -producers int
        Number of concurrent producer goroutines for higher throughput (default: 1)
```

### ProducerV2 Examples

**High-performance non-durable messages (fire-and-forget):**
```bash
./producerv2 -server localhost:5672 -queue testQueue
# Output: 1 Thread Throughput: 60000 msg/s
```

**Maximum throughput with multiple threads:**
```bash
./producerv2 -server localhost:5672 -queue testQueue -producers 10
# Output: 10 Threads Throughput: 38000 msg/s (total across all threads)
```

**Durable messages with reliability:**
```bash
./producerv2 -server localhost:5672 -queue testQueue -durable
# Output: 1 Thread Throughput: 4500 msg/s (limited by disk fsync)
```

**Cluster with failover:**
```bash
./producerv2 -server 192.168.1.10:5672,192.168.1.11:5672 -queue testQueue
```

**Load messages from file:**
```bash
./producerv2 -server localhost:5672 -queue testQueue -file messages.txt
```

### Performance Comparison

| Mode | Producer | ProducerV2 | Improvement |
|------|----------|------------|-------------|
| Non-durable (1 thread) | ~4,500 msg/s | ~60,000 msg/s | **13x faster** |
| Non-durable (10 threads) | ~32,000 msg/s | ~38,000 msg/s | 1.2x faster |
| Durable (1 thread) | ~4,400 msg/s | ~4,500 msg/s | Similar |
| Durable (10 threads) | ~31,000 msg/s | ~32,500 msg/s | Slightly better |

**Key Insight:** ProducerV2 is dramatically faster for non-durable messages due to `SenderSettleModeSettled` (fire-and-forget). For durable messages, both are limited by disk I/O.

## Consumer Usage

### Command Line Options

```bash
./consumer [OPTIONS]

Options:
  -server string
        The host and port of the ActiveMQ Artemis AMQP acceptor (required)
  -username string
        Username for authentication (default: "admin")
  -password string
        Password for authentication (default: "admin")
  -queue string
        The target queue where messages will be received from (default: "DLQ")
```

### Consumer Examples

**Consume from default queue:**
```bash
./consumer -server localhost:5672
```

**Consume from custom queue:**
```bash
./consumer -server localhost:5672 -queue testqueue
```

**Remote server:**
```bash
./consumer -server activemq.example.com:5672 -queue production.events
```

## DiskTest Usage

The DiskTest tool demonstrates why durable messaging has lower throughput than non-durable by measuring disk I/O performance with different block sizes and sync modes.

### Command Line Options

```bash
./disktest [OPTIONS]

Options:
  -blocksize <bytes>  Block size for each write operation (default: 4096)
  -total <bytes>      Total data to write in bytes (default: 104857600 = 100MB)
  -fsync              Call fsync after each write (simulates durable messages)
  -file <path>        Output file path (default: disktest.dat)
  -help               Show help and examples
```

### DiskTest Examples

**1. Test durable message performance (small blocks with fsync):**
```bash
./disktest -blocksize 1024 -total 10485760 -fsync
# Simulates: Each message persisted to disk immediately
# Expected: ~500-1000 msg/s (limited by disk IOPS)
```

**2. Test batched durable messages (64KB blocks with fsync):**
```bash
./disktest -blocksize 65536 -total 10485760 -fsync
# Simulates: ~64 messages batched per fsync
# Expected: Better throughput, same IOPS limit
```

**3. Test large batch durable messages (128KB blocks, sustained test):**
```bash
./disktest -blocksize 131072 -total 1073741824 -fsync
# Simulates: ~128 messages batched per fsync (1GB write)
# Expected: Even better throughput, maximizes IOPS efficiency, ~30 sec duration
```

**4. Test non-durable message performance (no fsync):**
```bash
./disktest -blocksize 1024 -total 10485760
# Simulates: Messages buffered in memory (no disk wait)
# Expected: 100,000+ msg/s (memory speed)
```

### DiskTest Results Interpretation

| Test | Block Size | Fsync | Throughput | IOPS | Messages/sec |
|------|-----------|-------|------------|------|--------------|
| Small + Fsync | 1 KB | Yes | 0.62 MB/s | 1,275 | **638 msg/s** |
| Large + Fsync | 128 KB | Yes | 36.98 MB/s | 592 | **296 msg/s** |
| No Fsync | 1 KB | No | 292 MB/s | 299,758 | **299,758 msg/s** |

**Understanding Results:**
- **IOPS**: I/O Operations Per Second (with fsync: write + sync operations)
- **Blocks/sec**: Number of write operations completed per second
- **Throughput**: Data transfer rate in MB/s
- **For durable messages**: max msg/s ≈ IOPS / 2

**Key Insight:** The disk fsync rate (~500-1,000/sec) is the fundamental bottleneck for durable messages. This is why:
- Durable messages: ~4,500 msg/s (limited by disk)
- Non-durable messages: ~60,000 msg/s (memory buffered)

## Output

Both tools display real-time throughput and final statistics:

```
Throughput: 15234 msg/s
Throughput: 15891 msg/s
Throughput: 15456 msg/s
^C
Shutdown complete.
Total messages sent: 453210
Overall average throughput: 15107.00 msg/s
```

## Benchmarking Workflow

### 1. Start the Consumer

```bash
./consumer -server localhost:5672 -queue benchmark
```

### 2. Start the Producer

In another terminal:
```bash
./producer -server localhost:5672 -queue benchmark -size 1024
```

### 3. Monitor Throughput

Both tools will display real-time throughput. The consumer throughput should closely match the producer when the system is keeping up.

### 4. Test Different Scenarios

**Small messages (high message rate):**
```bash
./producer -server localhost:5672 -queue benchmark -size 100
```

**Large messages (high data throughput):**
```bash
./producer -server localhost:5672 -queue benchmark -size 102400
```

**Multiple producers/consumers:**
```bash
# Terminal 1-3: Start 3 producers
./producer -server localhost:5672 -queue benchmark &
./producer -server localhost:5672 -queue benchmark &
./producer -server localhost:5672 -queue benchmark &

# Terminal 4-6: Start 3 consumers
./consumer -server localhost:5672 -queue benchmark &
./consumer -server localhost:5672 -queue benchmark &
./consumer -server localhost:5672 -queue benchmark &
```

## ActiveMQ Artemis Setup

### Enable AMQP Acceptor

Ensure AMQP is enabled in `broker.xml`:

```xml
<acceptors>
    <acceptor name="amqp">tcp://0.0.0.0:5672?protocols=AMQP</acceptor>
</acceptors>
```

### Create Queue

Using Artemis CLI:
```bash
./artemis queue create --name benchmark --address benchmark --anycast --auto-create-address
```

Or in `broker.xml`:
```xml
<addresses>
    <address name="benchmark">
        <anycast>
            <queue name="benchmark"/>
        </anycast>
    </address>
</addresses>
```

### Configure Users

Edit `artemis-users.properties`:
```properties
admin = admin
```

Edit `artemis-roles.properties`:
```properties
admin = admin
```

## Project Structure

```
activemq/
├── producer/
│   ├── main.go           # Producer implementation
│   ├── go.mod
│   └── go.sum
├── consumer/
│   ├── main.go           # Consumer implementation
│   ├── go.mod
│   └── go.sum
└── README.md
```

## Dependencies

```go
require (
    pack.ag/amqp v0.12.5              // Used by producer.go and consumer.go
    github.com/Azure/go-amqp v1.5.0   // Used by producerv2.go
)
```

**Producer vs ProducerV2:**
- `producer.go` uses the older `pack.ag/amqp` library
- `producerv2.go` uses the newer, actively maintained `github.com/Azure/go-amqp` library
- ProducerV2 achieves 13x better performance for non-durable messages using `SenderSettleModeSettled`

## Building

### Build Both Tools

```bash
# Build with optimizations
cd producer && go build -ldflags="-s -w" -o producer .
cd ../consumer && go build -ldflags="-s -w" -o consumer .
```

### Cross-Compile

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o producer-linux .
GOOS=linux GOARCH=amd64 go build -o consumer-linux .

# Windows
GOOS=windows GOARCH=amd64 go build -o producer.exe .
GOOS=windows GOARCH=amd64 go build -o consumer.exe .

# macOS ARM
GOOS=darwin GOARCH=arm64 go build -o producer-darwin-arm64 .
GOOS=darwin GOARCH=arm64 go build -o consumer-darwin-arm64 .
```

## Docker Support

### Producer Dockerfile

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY producer/go.* ./
RUN go mod download
COPY producer/ ./
RUN go build -ldflags="-s -w" -o producer .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/producer .
ENTRYPOINT ["./producer"]
```

### Consumer Dockerfile

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY consumer/go.* ./
RUN go mod download
COPY consumer/ ./
RUN go build -ldflags="-s -w" -o consumer .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/consumer .
ENTRYPOINT ["./consumer"]
```

### Run with Docker

```bash
# Build
docker build -f producer/Dockerfile -t activemq-producer .
docker build -f consumer/Dockerfile -t activemq-consumer .

# Run
docker run activemq-producer -server host.docker.internal:5672 -queue test
docker run activemq-consumer -server host.docker.internal:5672 -queue test
```

## Graceful Shutdown

Both tools support graceful shutdown via `Ctrl+C`:
1. Stops processing new messages
2. Closes AMQP receivers/senders
3. Closes sessions and connections
4. Displays final statistics

## Performance Tips

**Producer optimization:**
- Smaller payloads = higher message rate
- Larger payloads = higher data throughput (MB/s)
- Run multiple producers for maximum throughput
- Monitor broker memory and CPU usage

**Consumer optimization:**
- Messages are automatically acknowledged after receive
- Multiple consumers enable parallel processing
- Consumer throughput limited by message processing logic

**System tuning:**
- Increase broker memory (`-Xmx` in `artemis.profile`)
- Adjust prefetch settings in broker configuration
- Monitor network bandwidth between client and broker
- Use persistent vs non-persistent for different use cases

## Troubleshooting

**Connection refused**
- Verify ActiveMQ Artemis is running: `./artemis-service status`
- Check AMQP acceptor enabled on port 5672
- Test connectivity: `telnet localhost 5672`

**Authentication failed**
- Check credentials in `artemis-users.properties`
- Verify user roles in `artemis-roles.properties`
- Ensure security is properly configured in `broker.xml`

**Queue not found**
- Create queue before running tools
- Enable auto-create-queues in broker config
- Verify queue name is case-sensitive match

**Low throughput**
- Check broker CPU/memory usage
- Monitor network latency
- Verify disk I/O if using persistence
- Check for broker warnings in `artemis.log`

**Consumer lagging behind producer**
- Add more consumer instances
- Check consumer processing time
- Verify broker isn't hitting resource limits
- Monitor queue depth in admin console

## Monitoring

### ActiveMQ Artemis Console

Access at `http://localhost:8161/console`
- View queue depth
- Monitor message rates
- Check connection count
- View consumer details

### CLI Monitoring

```bash
# Check queue status
./artemis queue stat --name benchmark

# View connections
./artemis connection list

# Monitor addresses
./artemis address show
```

## License

[Specify your license - e.g., Apache License 2.0, MIT, etc.]

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit your changes (`git commit -m 'Add improvement'`)
4. Push to the branch (`git push origin feature/improvement`)
5. Open a Pull Request

## Contact

Tron - [@trondk](https://github.com/trondk)

Project Link: [https://github.com/trondk/activemq](https://github.com/trondk/activemq)

## Acknowledgments

- [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/)
- [pack.ag/amqp](https://pkg.go.dev/pack.ag/amqp) - AMQP 1.0 Go library
- [AMQP 1.0 Specification](http://www.amqp.org/specification/1.0/amqp-org-download)
