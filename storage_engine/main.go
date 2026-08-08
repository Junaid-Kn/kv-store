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

fmt.Println("=== PUT TEST ===")

// Insert some values.
err = s.Put([]byte("apple"), []byte("red"))
if err != nil {
	fmt.Println("Put error:", err)
	return
}

err = s.Put([]byte("banana"), []byte("yellow"))
if err != nil {
	fmt.Println("Put error:", err)
	return
}

err = s.Put([]byte("carrot"), []byte("orange"))
if err != nil {
	fmt.Println("Put error:", err)
	return
}

fmt.Println("Inserted apple, banana, carrot")

// Give the background flush goroutine time to finish.
time.Sleep(1 * time.Second)

fmt.Println("\n=== GET TEST ===")

value, err := s.Read([]byte("apple"))
if err != nil {
	fmt.Println("Read error:", err)
	return
}

fmt.Println("apple =", value)

value, err = s.Read([]byte("banana"))
if err != nil {
	fmt.Println("Read error:", err)
	return
}

fmt.Println("banana =", value)

value, err = s.Read([]byte("carrot"))
if err != nil {
	fmt.Println("Read error:", err)
	return
}

fmt.Println("carrot =", value)

fmt.Println("\n=== UPDATE TEST ===")

// Update an existing key.
err = s.Put([]byte("apple"), []byte("green"))
if err != nil {
	fmt.Println("Update error:", err)
	return
}

value, err = s.Read([]byte("apple"))
if err != nil {
	fmt.Println("Read error:", err)
	return
}

fmt.Println("apple after update =", value)

fmt.Println("\n=== SSTABLE TEST ===")

// Show the SSTable name that would be generated.
name := generateName(DataComponent, SSTExtension)
fmt.Println("Next SSTable:", name)

// Show current MemTable size.
fmt.Println("Current MemTable size:", s.MemTable.Size)

}


// have sorted data blocks on disk

// [keyLen][Key][ValLen][Value] [Index]


// Things to do:
// 1. Create the index for the SSTables
// 2. Durability for Memtables incase of crash 
// (create seperate log that appends every write that goes into MemTable)
// 3. Bloom Filter implementation for fast Lookup
// 4. Periodically merge the SSTable segments into 1 file and then delete the useless ones. 
