package main

import (
	// "bytes"
	"fmt"
)

func main() {

	sl := NewSkipList(16)

	sl.Insert([]byte("key1"), []byte("value1"))
	sl.Insert([]byte("key2"), []byte("value2"))
	sl.Insert([]byte("key3"), []byte("value3"))

	_, node := sl.Get([]byte("key2"))

	fmt.Println(string(node.Key))
	fmt.Println(string(node.Value))
}