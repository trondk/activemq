package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	inputFile := flag.String("file", "", "Path to the exported message IDs file")
	flag.Parse()

	if *inputFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastMessageNum int64 = -1
	var lineNum int64 = 0
	var outOfOrderCount int64 = 0
	var duplicateCount int64 = 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		messageNum, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			log.Printf("Line %d: Failed to parse message number '%s': %v", lineNum, line, err)
			continue
		}

		if lastMessageNum == -1 {
			// First message
			lastMessageNum = messageNum
		} else if messageNum == lastMessageNum {
			// Duplicate detected
			duplicateCount++
			log.Printf("Line %d: Duplicate message number %d (expected %d)", lineNum, messageNum, lastMessageNum+1)
		} else if messageNum == lastMessageNum+1 {
			// Correct sequence
			lastMessageNum = messageNum
		} else if messageNum > lastMessageNum {
			// Gap in sequence
			outOfOrderCount++
			log.Printf("Line %d: Gap in sequence - expected %d, got %d (missing %d messages)", lineNum, lastMessageNum+1, messageNum, messageNum-lastMessageNum-1)
			lastMessageNum = messageNum
		} else {
			// Message is out of order (earlier than expected)
			outOfOrderCount++
			log.Printf("Line %d: Out of order - expected %d or higher, got %d", lineNum, lastMessageNum+1, messageNum)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	// Print summary
	fmt.Println("\n=== Sequence Check Report ===")
	fmt.Printf("Total messages checked: %d\n", lineNum)
	fmt.Printf("Last message number: %d\n", lastMessageNum)
	fmt.Printf("Duplicate messages: %d\n", duplicateCount)
	fmt.Printf("Out of order / gaps: %d\n", outOfOrderCount)

	if outOfOrderCount == 0 && duplicateCount == 0 {
		fmt.Println("Status: ✓ All messages are in perfect sequence")
		os.Exit(0)
	} else {
		fmt.Println("Status: ✗ Sequence issues detected")
		os.Exit(1)
	}
}
