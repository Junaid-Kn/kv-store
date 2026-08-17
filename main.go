package main

import (
	"fmt"
	"os"
	"time"
	"github.com/Junaid-Kn/kv-store/storage_engine"
	"log"
	"path/filepath"
)

func main() {

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <data-directory>", os.Args[0])
	}
	dataDir, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	// Create a KVStorage with an empty MemTable.
	s, err := storage_engine.NewKVStorage(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	// Make sure the SSTable directory exists.
	// err = os.MkdirAll(s.SSTablesDir, 0755)
	// if err != nil {
	// 	fmt.Println("Failed to create SSTable directory:", err)
	// 	return
	// }

	fmt.Println("=== STRESS TEST: 10,000 KEYS ===")

	start := time.Now()

	// Insert 10,000 keys.
	for i := 0; i < 10000; i++ {

		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))

		err := s.Put(key, value)
		if err != nil {
			fmt.Printf("Put error at key %d: %v\n", i, err)
			return
		}
	}

	elapsed := time.Since(start)

	fmt.Printf("Inserted 10,000 keys in %v\n", elapsed)
	fmt.Printf("Current MemTable size: %d\n", s.MemTable.Size)

	// Give the background flush goroutine time to finish.
	time.Sleep(2 * time.Second)

	fmt.Println("\n=== READ TEST ===")

	// Test several keys throughout the dataset.
	testKeys := []int{
		0,
		1,
		100,
		1000,
		5000,
		9998,
		9999,
	}

	for _, i := range testKeys {

		key := []byte(fmt.Sprintf("key-%05d", i))

		start := time.Now()

		value, err := s.Read(key)

		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf(
				"Read key-%05d ERROR: %v (%v)\n",
				i,
				err,
				elapsed,
			)
			continue
		}
		fmt.Printf(
			"key-%05d = %s (%v)\n",
			i,
			value,
			elapsed,
		)
	}

	fmt.Println("\n=== MISSING KEY TEST ===")

	start = time.Now()

	_, err = s.Read([]byte("key-99999"))

	elapsed = time.Since(start)

	fmt.Printf(
		"Missing key result: %v (%v)\n",
		err,
		elapsed,
	)

	fmt.Println("\n=== SSTABLE TEST ===")

	name := storage_engine.GenerateName(storage_engine.DataComponent, storage_engine.SSTExtension, s.SSTablesDir )
	fmt.Println("Next SSTable:", name)

	fmt.Println("Current MemTable size:", s.MemTable.Size)

	fmt.Println("\n=== STRESS TEST COMPLETE ===")
}


// have sorted data blocks on disk

// [keyLen][Key][ValLen][Value] [Index]


// Things to do:
// 1. Wrote the sequenceNum to the kv_storage_engine and also each entry,
// 	 wrote sequence number to sstables as well as to WAL
// 2. Need to add Tombstones as well to each Entry
// 3. Need to also add compaction once the above 2 are implemented