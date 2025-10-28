package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"pack.ag/amqp"
)

func main() {
	serverAddr := flag.String("server", "", "The host and port of the ActiveMQ Artemis AMQP acceptor.")
	username := flag.String("username", "admin", "Username for authentication.")
	password := flag.String("password", "admin", "Password for authentication.")
	queueName := flag.String("queue", "HL7.ADT", "The target queue where HL7 messages will be sent.")
	inputFile := flag.String("file", "", "Input file containing HL7 messages (use '-' for stdin).")
	durable := flag.Bool("durable", true, "Set message delivery mode to Persistent (Durable).")
	flag.Parse()

	if *serverAddr == "" {
		fmt.Println("Error: -server flag is required")
		flag.Usage()
		os.Exit(1)
	}

	if *inputFile == "" {
		fmt.Println("Error: -file flag is required (use '-' for stdin)")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		cancel()
	}()

	// Connect to ActiveMQ Artemis
	client, err := amqp.Dial("amqp://"+*serverAddr, amqp.ConnSASLPlain(*username, *password))
	if err != nil {
		log.Fatalf("Failed to connect to AMQP server: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatalf("Failed to create AMQP session: %v", err)
	}

	sender, err := session.NewSender(amqp.LinkTargetAddress(*queueName))
	if err != nil {
		log.Fatalf("Failed to create AMQP sender: %v", err)
	}
	defer sender.Close(context.Background())

	var messagesSent atomic.Uint64
	startTime := time.Now()

	// Open input file or stdin
	var reader *bufio.Scanner
	if *inputFile == "-" {
		reader = bufio.NewScanner(os.Stdin)
		fmt.Println("Reading HL7 messages from stdin...")
	} else {
		file, err := os.Open(*inputFile)
		if err != nil {
			log.Fatalf("Failed to open input file: %v", err)
		}
		defer file.Close()
		reader = bufio.NewScanner(file)
		fmt.Printf("Reading HL7 messages from file: %s\n", *inputFile)
	}

	// Process HL7 messages
	err = processHL7Messages(ctx, reader, sender, *durable, &messagesSent)
	if err != nil {
		log.Fatalf("Error processing HL7 messages: %v", err)
	}

	// Calculate and print final summary
	totalTime := time.Since(startTime).Seconds()
	totalMessages := messagesSent.Load()
	avgThroughput := float64(totalMessages) / totalTime

	fmt.Println("\nImport complete.")
	fmt.Printf("Total messages sent: %d\n", totalMessages)
	fmt.Printf("Total time: %.2f seconds\n", totalTime)
	fmt.Printf("Average throughput: %.2f msg/s\n", avgThroughput)
}

func processHL7Messages(ctx context.Context, scanner *bufio.Scanner, sender *amqp.Sender, durable bool, messagesSent *atomic.Uint64) error {
	var currentMessage strings.Builder
	lineCount := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		lineCount++

		// Skip empty lines between messages
		if strings.TrimSpace(line) == "" {
			if currentMessage.Len() > 0 {
				// Send the accumulated message
				err := sendHL7Message(ctx, sender, currentMessage.String(), durable, messagesSent)
				if err != nil {
					return fmt.Errorf("failed to send message at line %d: %v", lineCount, err)
				}
				currentMessage.Reset()
			}
			continue
		}

		// Start of a new HL7 message (MSH segment)
		if strings.HasPrefix(line, "MSH|") {
			if currentMessage.Len() > 0 {
				// Send the previous message
				err := sendHL7Message(ctx, sender, currentMessage.String(), durable, messagesSent)
				if err != nil {
					return fmt.Errorf("failed to send message at line %d: %v", lineCount, err)
				}
				currentMessage.Reset()
			}
			currentMessage.WriteString(line)
		} else {
			// Continuation of current message
			if currentMessage.Len() > 0 {
				currentMessage.WriteString("\r")
				currentMessage.WriteString(line)
			}
		}
	}

	// Send the last message if any
	if currentMessage.Len() > 0 {
		err := sendHL7Message(ctx, sender, currentMessage.String(), durable, messagesSent)
		if err != nil {
			return fmt.Errorf("failed to send final message: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %v", err)
	}

	return nil
}

func sendHL7Message(ctx context.Context, sender *amqp.Sender, hl7Message string, durable bool, messagesSent *atomic.Uint64) error {
	// Validate that this is an HL7 message
	if !strings.HasPrefix(hl7Message, "MSH|") {
		return fmt.Errorf("invalid HL7 message: must start with MSH segment")
	}

	// Extract message type from MSH segment for logging
	segments := strings.Split(hl7Message, "\r")
	mshFields := strings.Split(segments[0], "|")
	var messageType string
	if len(mshFields) > 8 {
		messageType = mshFields[8]
	}

	// Create AMQP message with HL7 content
	message := amqp.NewMessage([]byte(hl7Message))

	// Set message properties
	message.Properties = &amqp.MessageProperties{
		ContentType: "application/hl7-v2",
	}

	// Set message as durable if requested
	if durable {
		message.Header = &amqp.MessageHeader{
			Durable: true,
		}
	}

	// Add application properties for filtering/routing
	message.ApplicationProperties = map[string]interface{}{
		"hl7.messageType": messageType,
		"hl7.version":     extractHL7Version(mshFields),
	}

	// Send the message
	err := sender.Send(ctx, message)
	if err != nil {
		return err
	}

	messagesSent.Add(1)
	fmt.Printf("Sent HL7 message: %s (Message ID: %s)\n", messageType, extractMessageControlID(mshFields))

	return nil
}

func extractHL7Version(mshFields []string) string {
	if len(mshFields) > 11 {
		return mshFields[11]
	}
	return "unknown"
}

func extractMessageControlID(mshFields []string) string {
	if len(mshFields) > 9 {
		return mshFields[9]
	}
	return "unknown"
}
