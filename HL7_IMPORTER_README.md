# HL7 ADT Message Importer for ActiveMQ Artemis

This tool imports HL7 ADT^A01 messages into ActiveMQ Artemis via AMQP protocol.

## Building

```bash
go build -o hl7_importer hl7_importer.go
```

## Usage

### Basic Usage

```bash
./hl7_importer -server localhost:5672 -file sample_hl7.txt -queue HL7.ADT
```

### Read from stdin

```bash
cat sample_hl7.txt | ./hl7_importer -server localhost:5672 -file - -queue HL7.ADT
```

### Command Line Options

- `-server` (required): The host and port of the ActiveMQ Artemis AMQP acceptor (e.g., `localhost:5672`)
- `-file` (required): Input file containing HL7 messages. Use `-` to read from stdin
- `-queue` (optional): The target queue where HL7 messages will be sent (default: `HL7.ADT`)
- `-username` (optional): Username for authentication (default: `admin`)
- `-password` (optional): Password for authentication (default: `admin`)
- `-durable` (optional): Set message delivery mode to Persistent/Durable (default: `true`)

## HL7 Message Format

The importer expects HL7 messages in the standard format with:
- Each segment on a separate line
- MSH segment starting the message
- Empty lines between multiple messages (optional)

Example:
```
MSH|^~\&|Epic|HGH|Other|PRIMARY|20250911101355|SEAKH|ADT^A01^ADT^A01|179186|T|2.5
EVN|A04|20250911101355||ED_AFTER_ARRIVAL|SEAKH^TEST^LÆGESEKRETÆR-AKUTMODTAGELSE-RGH^^^^^^REGH^^^^^HGH 36.
PID|1||1111111111^^^CPR^CPR||Sigård^Tenna||20160901|F|||Sigård Alle 1^^Harestrup^83^6100^DK^P^^510^Haderslev|510||||U||1516001001503^^^KONTAKT^HARID|0109169994|||||||||Ukendt||N

MSH|^~\&|Epic|HGH|Other|PRIMARY|20250911101400|SEAKH|ADT^A01^ADT^A01|179187|T|2.5
...
```

## Features

- Parses multi-segment HL7 messages
- Converts newlines to carriage returns (\r) as per HL7 standard
- Sets proper AMQP message properties:
  - Content-Type: `application/hl7-v2`
  - Application properties for message type and version
- Supports durable/persistent message delivery
- Validates messages start with MSH segment
- Displays progress and throughput statistics

## Message Properties

Each message sent to ActiveMQ Artemis includes:
- **Content-Type**: `application/hl7-v2`
- **Application Properties**:
  - `hl7.messageType`: The message type from MSH-9 (e.g., `ADT^A01^ADT^A01`)
  - `hl7.version`: The HL7 version from MSH-12 (e.g., `2.5`)
- **Durable**: Set to true by default for message persistence

## Example Output

```
Reading HL7 messages from file: sample_hl7.txt
Sent HL7 message: ADT^A01^ADT^A01 (Message ID: 179186)

Import complete.
Total messages sent: 1
Total time: 0.15 seconds
Average throughput: 6.67 msg/s
```

## Testing

A sample HL7 message file is provided in `sample_hl7.txt` for testing purposes.
