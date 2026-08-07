package main 

import ("math/rand/v2"
		"fmt"
		"bytes"
	)

type SkipList struct{
	HeadNode *SkipListNode
	MaxHeight int
	CurrentLevel int // current highest level 
}

type SkipListNode struct{
	Key []byte
	Value []byte
	Level int
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
			bytes.Compare(Key, curr.NextNode.Key) >= 0 {
			curr = curr.NextNode
		}

		// If we found the key
		if bytes.Equal(curr.Key, Key) {
			return update, curr
		}

		update[currHeight] = curr

		curr = curr.DownNode
		currHeight--
	}

	return update, nil
}


func (sl *SkipList) Insert(Key, Value []byte) error {

	updateList, keyNode := sl.Get(Key)

	if keyNode != nil {
		fmt.Println("Key exists, abort")
		return nil
	}

	// create bottom level node
	skipListNode := new(SkipListNode)

	skipListNode.Key = Key
	skipListNode.Value = Value
	skipListNode.Level = 0

	skipListNode.NextNode = updateList[0].NextNode
	updateList[0].NextNode = skipListNode

	curr := skipListNode
	idx := 1


	for coinFlip() == "Heads" && idx < sl.MaxHeight {
		newSkipListNode := new(SkipListNode)
		newSkipListNode.Key = Key
		newSkipListNode.Value = Value
		newSkipListNode.Level = idx

		newSkipListNode.NextNode = updateList[idx].NextNode
		updateList[idx].NextNode = newSkipListNode

		newSkipListNode.DownNode = curr

		curr = newSkipListNode

		idx++
	}


	// update current height if this node created a new level
	if idx-1 > sl.CurrentLevel {
		sl.CurrentLevel = idx-1
	}


	return nil
}

//private function
func coinFlip() string{
	if rand.IntN(2) == 0{
		return "Heads"
	}
	return "Tails"
}


func (sl *SkipList) Delete(Key []byte) error {
	
	return nil 
}