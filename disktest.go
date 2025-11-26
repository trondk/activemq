package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func printUsage() {
	fmt.Println("Disk Performance Test Tool")
	fmt.Println("===========================")
	fmt.Println()
	fmt.Println("This tool measures disk write performance with different block sizes and sync modes.")
	fmt.Println("It helps demonstrate why durable messaging has lower throughput than non-durable.")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  disktest [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -blocksize <bytes>  Block size for each write operation (default: 4096)")
	fmt.Println("  -total <bytes>      Total data to write in bytes (default: 104857600 = 100MB)")
	fmt.Println("  -fsync              Call fsync after each write (simulates durable messages)")
	fmt.Println("  -file <path>        Output file path (default: disktest.dat)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println()
	fmt.Println("  1. Test durable message performance (small blocks with fsync):")
	fmt.Println("     ./disktest -blocksize 1024 -total 10485760 -fsync")
	fmt.Println("     Simulates: Each message persisted to disk immediately")
	fmt.Println("     Expected: ~500-1000 msg/s (limited by disk IOPS)")
	fmt.Println()
	fmt.Println("  2. Test batched durable messages (64KB blocks with fsync):")
	fmt.Println("     ./disktest -blocksize 65536 -total 10485760 -fsync")
	fmt.Println("     Simulates: ~64 messages batched per fsync")
	fmt.Println("     Expected: Better throughput, same IOPS limit")
	fmt.Println()
	fmt.Println("  3. Test large batch durable messages (128KB blocks with fsync, sustained test):")
	fmt.Println("     ./disktest -blocksize 131072 -total 1073741824 -fsync")
	fmt.Println("     Simulates: ~128 messages batched per fsync (1GB write)")
	fmt.Println("     Expected: Even better throughput, maximizes IOPS efficiency, ~30 sec duration")
	fmt.Println()
	fmt.Println("  4. Test non-durable message performance (no fsync):")
	fmt.Println("     ./disktest -blocksize 1024 -total 10485760")
	fmt.Println("     Simulates: Messages buffered in memory (no disk wait)")
	fmt.Println("     Expected: 100,000+ msg/s (memory speed)")
	fmt.Println()
	fmt.Println("  5. Quick IOPS test (small write, fsync enabled):")
	fmt.Println("     ./disktest -blocksize 512 -total 1048576 -fsync")
	fmt.Println("     Simulates: Maximum IOPS measurement")
	fmt.Println("     Expected: Shows your disk's IOPS limit")
	fmt.Println()
	fmt.Println("Understanding Results:")
	fmt.Println("  - IOPS: I/O Operations Per Second (with fsync: write + sync operations)")
	fmt.Println("  - Blocks/sec: Number of write operations completed per second")
	fmt.Println("  - Throughput: Data transfer rate in MB/s")
	fmt.Println("  - For durable messages: max msg/s ≈ IOPS / 2")
	fmt.Println()
}

func main() {
	blockSize := flag.Int("blocksize", 4096, "Block size in bytes for write operations")
	totalSize := flag.Int("total", 100*1024*1024, "Total size to write in bytes (default 100MB)")
	fsync := flag.Bool("fsync", false, "Call fsync after each write (simulate durable messages)")
	filename := flag.String("file", "disktest.dat", "Output file for test")
	help := flag.Bool("help", false, "Show help and examples")

	flag.Parse()

	if *help {
		printUsage()
		os.Exit(0)
	}

	// Calculate number of blocks
	numBlocks := *totalSize / *blockSize

	fmt.Printf("Disk Write Test\n")
	fmt.Printf("===============\n")
	fmt.Printf("Block size:    %d bytes\n", *blockSize)
	fmt.Printf("Total size:    %d bytes (%.2f MB)\n", *totalSize, float64(*totalSize)/(1024*1024))
	fmt.Printf("Num blocks:    %d\n", numBlocks)
	fmt.Printf("Fsync mode:    %v\n", *fsync)
	fmt.Printf("Output file:   %s\n\n", *filename)

	// Create test data
	data := make([]byte, *blockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Open file for writing
	file, err := os.Create(*filename)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()
	defer os.Remove(*filename) // Clean up test file

	// Perform write test
	startTime := time.Now()
	var bytesWritten int64

	for i := 0; i < numBlocks; i++ {
		n, err := file.Write(data)
		if err != nil {
			fmt.Printf("Error writing: %v\n", err)
			os.Exit(1)
		}
		bytesWritten += int64(n)

		// Simulate durable message behavior
		if *fsync {
			err = file.Sync()
			if err != nil {
				fmt.Printf("Error syncing: %v\n", err)
				os.Exit(1)
			}
		}

		// Progress reporting every 10%
		if i > 0 && i%(numBlocks/10) == 0 {
			elapsed := time.Since(startTime).Seconds()
			throughput := float64(bytesWritten) / elapsed / (1024 * 1024)
			fmt.Printf("Progress: %3d%% - %.2f MB/s\n", (i*100)/numBlocks, throughput)
		}
	}

	// Final sync if not syncing after each write
	if !*fsync {
		file.Sync()
	}

	// Calculate results
	elapsed := time.Since(startTime).Seconds()
	throughputMB := float64(bytesWritten) / elapsed / (1024 * 1024)
	throughputBlocks := float64(numBlocks) / elapsed

	// IOPS calculation
	// Each write operation is 1 I/O, and if fsync is enabled, each fsync is also 1 I/O
	var iops float64
	if *fsync {
		// With fsync: write + fsync = 2 I/O operations per block
		iops = throughputBlocks * 2
	} else {
		// Without fsync: only write operations count (but they're buffered)
		iops = throughputBlocks
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("========\n")
	fmt.Printf("Total time:        %.3f seconds\n", elapsed)
	fmt.Printf("Bytes written:     %d bytes\n", bytesWritten)
	fmt.Printf("Throughput:        %.2f MB/s\n", throughputMB)
	fmt.Printf("Blocks/sec:        %.0f blocks/s\n", throughputBlocks)
	fmt.Printf("IOPS:              %.0f operations/s\n", iops)

	if *fsync {
		fmt.Printf("Fsync calls/sec:   %.0f/s\n", throughputBlocks)
		fmt.Printf("\nNote: With fsync, each write waits for disk persistence.\n")
		fmt.Printf("IOPS includes both write and fsync operations (2x blocks/s).\n")
		fmt.Printf("This simulates durable message behavior (~%0.f msg/s max).\n", throughputBlocks)
	} else {
		fmt.Printf("\nNote: Without fsync, writes are buffered in OS cache.\n")
		fmt.Printf("IOPS are buffered operations (not actual disk I/O).\n")
		fmt.Printf("This simulates non-durable message behavior (much faster).\n")
	}
}
