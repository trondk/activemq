package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type ConnectionManagerIBM struct {
	servers     []string
	queueName   string
	channelName string
	qMgrName    string
	currentIdx  int
	mu          sync.RWMutex
	qMgr        ibmmq.MQQueueManager
	qObject     ibmmq.MQObject
	connected   bool
	debug       bool
}

func main() {
	serverAddr := flag.String("server", "", "Comma-separated list of IBM MQ connection names (e.g., 'localhost(1414),host2(1414)').")
	msgSize := flag.Int("size", 1024, "The size of the payload for each message sent.")
	channelName := flag.String("channel", "DEV.APP.SVRCONN", "The channel name for IBM MQ connection.")
	qMgrName := flag.String("qmgr", "QM1", "The queue manager name.")
	queueName := flag.String("queue", "DEV.QUEUE.1", "The target queue where messages will be sent.")
	durable := flag.Bool("durable", false, "Set message delivery mode to Persistent (Durable).")
	producers := flag.Int("producers", 1, "Number of concurrent producer goroutines for higher throughput.")
	debug := flag.Bool("debug", false, "Enable debug logging for connection errors.")
	flag.Parse()

	if *serverAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	servers := strings.Split(*serverAddr, ",")

	// Trim spaces and convert format from host:port to host(port) if needed
	for i := range servers {
		servers[i] = strings.TrimSpace(servers[i])
		// Convert localhost:1414 format to localhost(1414) format
		if strings.Contains(servers[i], ":") && !strings.Contains(servers[i], "(") {
			parts := strings.Split(servers[i], ":")
			if len(parts) == 2 {
				servers[i] = parts[0] + "(" + parts[1] + ")"
			}
		}
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

	connMgr := &ConnectionManagerIBM{
		servers:     servers,
		queueName:   *queueName,
		channelName: *channelName,
		qMgrName:    *qMgrName,
		debug:       *debug,
	}

	// Initial connection
	if err := connMgr.Connect(ctx); err != nil {
		log.Fatalf("Failed to establish initial connection: %v", err)
	}
	defer connMgr.Close()

	var messagesSent atomic.Uint64
	startTime := time.Now()

	// Start multiple producer goroutines for concurrent sending
	for i := 0; i < *producers; i++ {
		go producer(ctx, connMgr, *msgSize, *durable, &messagesSent)
	}

	// Start the throughput reporter goroutine
	go throughputReporter(ctx, &messagesSent)

	<-ctx.Done()

	// Calculate and print final summary
	totalTime := time.Since(startTime).Seconds()
	totalMessages := messagesSent.Load()
	avgThroughput := float64(totalMessages) / totalTime

	fmt.Println("\nShutdown complete.")
	fmt.Printf("Total messages sent: %d\n", totalMessages)
	fmt.Printf("Overall average throughput: %.2f msg/s\n", avgThroughput)
}

func (cm *ConnectionManagerIBM) Connect(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Try all servers in the cluster
	for i := 0; i < len(cm.servers); i++ {
		serverIdx := (cm.currentIdx + i) % len(cm.servers)
		server := cm.servers[serverIdx]

		if cm.debug {
			log.Printf("Attempting to connect to %s (channel: %s, qmgr: %s)...", server, cm.channelName, cm.qMgrName)
		} else {
			log.Printf("Attempting to connect to %s...", server)
		}

		// Create connection options
		cno := ibmmq.NewMQCNO()
		cd := ibmmq.NewMQCD()

		// Configure channel definition
		cd.ChannelName = cm.channelName
		cd.ConnectionName = server

		// Set client binding mode
		cno.Options = ibmmq.MQCNO_CLIENT_BINDING

		// Assign the channel definition to connection options
		cno.ClientConn = cd

		// Connect to queue manager
		qMgr, err := ibmmq.Connx(cm.qMgrName, cno)
		if err != nil {
			if cm.debug {
				mqret := err.(*ibmmq.MQReturn)
				log.Printf("Failed to connect to %s: %v (MQRC: %d)", server, err, mqret.MQRC)
			} else {
				log.Printf("Failed to connect to %s: %v", server, err)
			}
			continue
		}

		// Open the queue
		mqod := ibmmq.NewMQOD()
		openOptions := ibmmq.MQOO_OUTPUT

		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = cm.queueName

		qObject, err := qMgr.Open(mqod, openOptions)
		if err != nil {
			if cm.debug {
				mqret := err.(*ibmmq.MQReturn)
				log.Printf("Failed to open queue %s on %s: %v (MQRC: %d)", cm.queueName, server, err, mqret.MQRC)
			} else {
				log.Printf("Failed to open queue %s on %s: %v", cm.queueName, server, err)
			}
			qMgr.Disc()
			continue
		}

		cm.qMgr = qMgr
		cm.qObject = qObject
		cm.currentIdx = serverIdx
		cm.connected = true
		log.Printf("Successfully connected to %s", server)
		return nil
	}

	return fmt.Errorf("failed to connect to any server in the cluster")
}

func (cm *ConnectionManagerIBM) Reconnect(ctx context.Context) error {
	cm.mu.Lock()
	// Close existing connections
	if cm.connected {
		if cm.qObject.Name != "" {
			cm.qObject.Close(0)
		}
		cm.qMgr.Disc()
		cm.connected = false
	}
	// Move to next server
	cm.currentIdx = (cm.currentIdx + 1) % len(cm.servers)
	cm.mu.Unlock()

	// Wait a bit before reconnecting
	time.Sleep(1 * time.Second)

	return cm.Connect(ctx)
}

func (cm *ConnectionManagerIBM) GetConnection() (ibmmq.MQQueueManager, ibmmq.MQObject, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.qMgr, cm.qObject, cm.connected
}

func (cm *ConnectionManagerIBM) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.connected {
		if cm.qObject.Name != "" {
			cm.qObject.Close(0)
		}
		cm.qMgr.Disc()
		cm.connected = false
	}
}

func producer(ctx context.Context, connMgr *ConnectionManagerIBM, msgSize int, durable bool, messagesSent *atomic.Uint64) {
	// Generate and send dummy payload continuously
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = 'X'
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, qObject, connected := connMgr.GetConnection()
			if !connected {
				log.Printf("No active connection, attempting to reconnect...")
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			// Create message descriptor and put options
			putmqmd := ibmmq.NewMQMD()
			pmo := ibmmq.NewMQPMO()

			// Set message format
			putmqmd.Format = ibmmq.MQFMT_STRING

			if durable {
				putmqmd.Persistence = ibmmq.MQPER_PERSISTENT
			} else {
				putmqmd.Persistence = ibmmq.MQPER_NOT_PERSISTENT
			}

			err := qObject.Put(putmqmd, pmo, payload)
			if err != nil {
				log.Printf("Failed to send message: %v, attempting reconnection...", err)
				if err := connMgr.Reconnect(ctx); err != nil {
					log.Printf("Reconnection failed: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			messagesSent.Add(1)
		}
	}
}

func throughputReporter(ctx context.Context, messagesSent *atomic.Uint64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCount uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentCount := messagesSent.Load()
			throughput := currentCount - lastCount
			lastCount = currentCount
			fmt.Printf("Throughput: %d msg/s\n", throughput)
		}
	}
}
