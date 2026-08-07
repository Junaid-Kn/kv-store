package main 

import ("math/rand/v2")

type SkipList struct{
	HeadNode *SkipListNode
	MaxHeight uint8
}

type SkipListNode struct{
	Key []byte
	Value []byte

	// Pointer to the next node
	NextNode *SkipListNode
	DownNode *SkipListNode
}

func (sl *SkipList) Get(Key[]byte) ([]SkipListNode, *SkipListNode){
	update := make([]SkipListNode{}, curr.MaxHeight)
	curr := sl.HeadNode
	currHeight = curr.MaxHeight
	for curr.DownNode != nil || curr.Key == Key {
		
		for key < curr.NextNode.Key && curr.DownNode != nil{
			curr = curr.DownNode
			currHeight -= 1 
		}
		if key >= curr.NextNode.Key{
			
			curr = curr.NextNode 	
			update[currHeight] = curr

		}
	}
	if curr.Key == Key{
		return nil, nil
	}
	return update, curr

}

func (sl *SkipList) Insert(Key, Value []byte) error { 
	
	updateList, keyNode = sl.Get(Key)
	if keyNode != nil{
		fmt.Println("Key exists, abort")
		return nil 
	}

	skipListNode = new(SkipListNode)
	skipListNode.Key = Key
	skipListNode.Value = Value
	keyNode.NextNode = skipListNode

	curr = skipListNode
	idx := 1
	for coinFlip() == "Heads"{
		newSkipListNode = new(SkipListNode)
		newSkipListNode.Key = Key
		newSkipListNode.Value = Value

		curr.DownNode = newSkipListNode
		curr = curr.DownNode
	}

	//update the connections of all the previous nodes 

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