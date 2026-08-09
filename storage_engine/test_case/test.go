package storage_engine

import (
	"fmt"
)

func test() {

	// Create the KV store
	s := KVStorage{
		MemTable: *NewSkipList(16),
	}

	// --------------------------------
	// Test 1: Insert a new key
	// --------------------------------

	err := s.Put(
		[]byte("key1"),
		[]byte("value1"),
	)

	if err != nil {
		fmt.Println("PUT key1 failed:", err)
		return
	}

	fmt.Println("PUT key1: PASSED")


	// --------------------------------
	// Test 2: Read the key
	// --------------------------------

	value, err := s.Read([]byte("key1"))

	if err != nil {
		fmt.Println("READ key1 failed:", err)
		return
	}

	fmt.Println("READ key1:", value)

	if value != "value1" {
		fmt.Println("READ key1: FAILED")
		return
	}

	fmt.Println("READ key1: PASSED")


	// --------------------------------
	// Test 3: Update existing key
	// --------------------------------

	err = s.Put(
		[]byte("key1"),
		[]byte("value2"),
	)

	if err != nil {
		fmt.Println("UPDATE key1 failed:", err)
		return
	}

	fmt.Println("UPDATE key1: PASSED")


	// --------------------------------
	// Test 4: Read updated value
	// --------------------------------

	value, err = s.Read([]byte("key1"))

	if err != nil {
		fmt.Println("READ updated key1 failed:", err)
		return
	}

	fmt.Println("READ updated key1:", value)

	if value != "value2" {
		fmt.Println("READ updated key1: FAILED")
		return
	}

	fmt.Println("READ updated key1: PASSED")


	// --------------------------------
	// Test 5: Insert another key
	// --------------------------------

	err = s.Put(
		[]byte("key2"),
		[]byte("hello"),
	)

	if err != nil {
		fmt.Println("PUT key2 failed:", err)
		return
	}

	fmt.Println("PUT key2: PASSED")


	// --------------------------------
	// Test 6: Read second key
	// --------------------------------

	value, err = s.Read([]byte("key2"))

	if err != nil {
		fmt.Println("READ key2 failed:", err)
		return
	}

	fmt.Println("READ key2:", value)

	if value != "hello" {
		fmt.Println("READ key2: FAILED")
		return
	}

	fmt.Println("READ key2: PASSED")


	fmt.Println("\nAll KVStore tests passed!")
}