package storage_engine

import (
	"bytes"
	"fmt"
	"math/rand/v2"
)


type SkipList struct {
	HeadNode    *SkipListNode
	MaxHeight   int
	CurrentLevel int // current highest level
	Size uint64
}

type Entry struct {
	Key   []byte
	Value []byte
	SequenceNum uint64
}

type SkipListNode struct {
	// Record stores the entry of key and value pair
	Record *Entry
	Level  int

	// Pointer to the next node
	NextNode *SkipListNode
	DownNode *SkipListNode
}

func (sl *SkipList) Get(Key []byte) ([]*SkipListNode, *SkipListNode) {

	update := make([]*SkipListNode, sl.MaxHeight)

	curr := sl.HeadNode
	currHeight := sl.CurrentLevel

	for curr != nil {

		for curr.NextNode != nil &&
			bytes.Compare(Key, curr.NextNode.Record.Key) >= 0 {
			curr = curr.NextNode
		}

		// If we found the key
		if curr.Record != nil && bytes.Equal(curr.Record.Key, Key) {
			return update, curr
		}

		update[currHeight] = curr

		curr = curr.DownNode
		currHeight--
	}

	return update, nil
}

func (sl *SkipList) GetRecord(Key[]byte) *Entry { 
	_, node := sl.Get(Key)
	if node != nil { 
		return node.Record
	}
	return nil
}

func (sl *SkipList) Insert(Record *Entry) error {

	updateList, keyNode := sl.Get(Record.Key)

	if keyNode != nil {
		fmt.Println("Key exists, abort")
		return nil
	}

	// Add the actual key/value data to the MemTable size.
	sl.Size += uint64(len(Record.Key))
	sl.Size += uint64(len(Record.Value))

	// Create bottom level node
	skipListNode := new(SkipListNode)

	skipListNode.Record = Record
	skipListNode.Level = 0

	skipListNode.NextNode = updateList[0].NextNode
	updateList[0].NextNode = skipListNode

	curr := skipListNode
	idx := 1

	for coinFlip() == "Heads" && idx < sl.MaxHeight {

		newSkipListNode := new(SkipListNode)

		// Same record as the node below
		newSkipListNode.Record = Record
		newSkipListNode.Level = idx

		newSkipListNode.NextNode = updateList[idx].NextNode
		updateList[idx].NextNode = newSkipListNode

		newSkipListNode.DownNode = curr

		curr = newSkipListNode

		idx++
	}

	// Update current height if this node created a new level
	if idx-1 > sl.CurrentLevel {
		sl.CurrentLevel = idx - 1
	}

	return nil
}

func (sl *SkipList) Update(Key, Value []byte) error {

	_, keyNode := sl.Get(Key)

	if keyNode == nil {
		return fmt.Errorf("key not found")
	}

	oldValueSize := len(keyNode.Record.Value)

	newValue := append([]byte(nil), Value...)

	// Adjust size based on the difference between
	// the old value and the new value.
	sl.Size -= uint64(oldValueSize)
	sl.Size += uint64(len(newValue))

	keyNode.Record.Value = newValue

	return nil
}

// private function
func coinFlip() string {
	if rand.IntN(2) == 0 {
		return "Heads"
	}
	return "Tails"
}

func (sl *SkipList) Delete(Key []byte) error {

	return nil
}

func NewSkipList(maxHeight int) *SkipList {

	var down *SkipListNode

	for i := 0; i < maxHeight; i++ {

		head := &SkipListNode{
			Level: i,
		}

		head.DownNode = down
		down = head
	}

	return &SkipList{
		HeadNode:     down,
		MaxHeight:    maxHeight,
		CurrentLevel: maxHeight - 1,
	}
}