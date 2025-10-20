# Go AMQP Producer for ActiveMQ Artemis

## Project Goal

This program is a high-performance Go-based AMQP producer designed to continuously send messages to an ActiveMQ Artemis queue using the AMQP 1.0 protocol.

## Features

*   Connects to an ActiveMQ Artemis broker using AMQP 1.0.
*   Authenticates using SASL PLAIN.
*   Sends messages to a specified queue.
*   Message size can be configured via command-line flags.
*   Reports throughput (messages per second) every second.
*   Supports graceful shutdown using Ctrl+C.
*   Prints a summary of total messages sent and average throughput on shutdown.

## Prerequisites

*   Go 1.18 or later installed.
*   An ActiveMQ Artemis server running and accessible from the machine where you are running the producer.
*   The target queue must exist on the ActiveMQ Artemis server.

## How to Run

To run the producer, use the `go run` command followed by the path to the `producer.go` file. You can use the command-line flags to customize the producer's behavior.

```bash
go run producer.go [flags]
```

## Command-line Flags

| Flag       | Description                                               | Default Value        |
|------------|-----------------------------------------------------------|----------------------|
| `-server`  | The host and port of the ActiveMQ Artemis AMQP acceptor.  | (required)           |
| `-size`    | The size of the payload for each message sent (in bytes). | 1024                 |
| `-username`| Username for authentication.                              | `admin`              |
| `-password`| Password for authentication.                              | `admin`              |
| `-queue`   | The target queue where messages will be sent.             | `DLQ`                |

## Example Usage

```bash
go run producer.go -server 192.168.100.10:5672 -queue my-queue -size 2048
```

## Graceful Shutdown

To gracefully shut down the producer, press `Ctrl+C`. The producer will stop sending messages, close the AMQP connection, and print a final summary.

## Output

While running, the producer will print the throughput every second:

```
Throughput: 1500 msg/s
Throughput: 1480 msg/s
```

When you shut down the producer, it will print a summary like this:

```

Shutdown complete.
Total messages sent: 14900
Overall average throughput: 1490.00 msg/s
```
