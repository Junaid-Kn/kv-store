package main

import (
	"fmt"
	"os"
	"time"
)

func main() {

	// Make sure the SSTable directory exists.
	err := os.MkdirAll(SSTABLES_DIR, 0755)
	if err != nil {
		fmt.Println("Failed to create SSTable directory:", err)
		return
	}

	// Create a KVStorage with an empty MemTable.
	s := &KVStorage{
		MemTable: NewSkipList(16),
	}

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

	name := generateName(DataComponent, SSTExtension)
	fmt.Println("Next SSTable:", name)

	fmt.Println("Current MemTable size:", s.MemTable.Size)

	fmt.Println("\n=== STRESS TEST COMPLETE ===")
}


// have sorted data blocks on disk

// [keyLen][Key][ValLen][Value] [Index]


// Things to do:
// 1. Create the index for the SSTables
// 2. Durability for Memtables incase of crash 
// (create seperate log that appends every write that goes into MemTable)
// 3. Bloom Filter implementation for fast Lookup
// 4. Periodically merge the SSTable segments into 1 file and then delete the useless ones. 
