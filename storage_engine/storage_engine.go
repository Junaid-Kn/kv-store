package main
import (
	// "errors"
	"os"
	"sync"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"strconv"
)

type SSTableComponent string
type SSTableExtension string

const (
	DataComponent   SSTableComponent = "data"
	IndexComponent  SSTableComponent = "index"
	MetaComponent   SSTableComponent = "meta"
)

const (
	SSTExtension SSTableExtension = "sst"
	IndexExtension SSTableExtension = "idx"
)

const MAX_SIZE_BEFORE_FLUSH = 64 * 1024 * 1024
const SSTABLES_DIR = "./sstables"


type Engine interface {
    Put(Key, Value []byte) error
    Get(Key []byte) ([]byte, error)     // returns ErrNotFound if missing
    // Delete(Key []byte) error
    // Close() error                        // flush + close files cleanly
}




type KVStorage struct { 
	Mu sync.RWMutex
	MemTable *SkipList

	WALFile *os.File
}

func (s * KVStorage) WriteToWAL(Op uint8, Key, Value[]byte) error { 
	f, err := OpenOrCreateFile("WAL.bin")
	if err != nil { 
		return err
	}
	defer f.Close()

	curr_counter, err := LoadCounter("counter.bin")
	if err != nil { 
		fmt.Println(err)
		curr_counter = 0 
		// create the first counter in the file 
		SaveCounter(0)
	}
	curr_counter += 1
	WALRecord := WALInput { 
		TransactionId: uint64(curr_counter),
		Op: Op, // INSERT,PUT, GET, DELETE
		Key: Key,
		KeyLen: uint16(len(Key)),
		Value: Value,
		ValueLen: uint32(len(Value)),
		CheckSum:0, // do checksum calc later
	}

	data := []byte(fmt.Sprintf("%d%d%d", WALRecord.Key, WALRecord.Value, WALRecord.TransactionId))
	WALRecord.CheckSum = crc32.ChecksumIEEE(data)
	
	// write to the file
	binary.Write(f, binary.LittleEndian, WALRecord.TransactionId)
	binary.Write(f, binary.LittleEndian, WALRecord.Op)
	binary.Write(f, binary.LittleEndian, WALRecord.KeyLen)
	binary.Write(f, binary.LittleEndian, WALRecord.Key)
	binary.Write(f, binary.LittleEndian, WALRecord.ValueLen)
	binary.Write(f, binary.LittleEndian, WALRecord.Value)
	binary.Write(f, binary.LittleEndian, WALRecord.CheckSum)
	// write to the counter.bin file 
	SaveCounter(int64(WALRecord.TransactionId))

	return nil
}



func (s * KVStorage) Put (Key,Value []byte) error {
	s.Mu.Lock()
	err := s.WriteToWAL(2, Key, Value);
	if err != nil{
		return err 
	}

	// write to memtable
	_, node := s.MemTable.Get(Key)
	  if node != nil {
        err = s.MemTable.Update(Key, Value)
    } else {
        err = s.MemTable.Insert(Key, Value)
    }

	if err != nil {
		return err
	}

	if s.MemTable.Size >= MAX_SIZE_BEFORE_FLUSH{
		// freeze current memTable and then flush it into SSTable
		newMemTable := NewSkipList(16)
		oldMemTable := s.MemTable
		s.MemTable = newMemTable
		s.Mu.Unlock()
		f, err := os.OpenFile(generateName(DataComponent, SSTExtension), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		defer f.Close()

		go func () {
			// get to base node of the dummy node tower
			curr := oldMemTable.HeadNode 
			for curr.DownNode != nil {
				curr = curr.DownNode
			}
			// traverse next for every single element in the list
			curr = curr.NextNode
			for curr != nil { 
				key := curr.Record.Key
				value := curr.Record.Value
					
				binary.Write(f, binary.LittleEndian, uint32(len(key)))
				f.Write(key)

				binary.Write(f, binary.LittleEndian, uint32(len(value)))
				f.Write(value)

				curr = curr.NextNode
				
			}
		
		}()
	}
	s.Mu.Unlock()
	return nil 

}

func ( s * KVStorage) Read(Key []byte) (string, error) { 
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	err := s.WriteToWAL(3, Key, []byte(""));
	if err != nil{
		return "", err 
	}
	
	
	_, node := s.MemTable.Get(Key)
	
	if node != nil  { 
		return string(node.Record.Value), nil
	}else{
		// read from disk 
		return "123", nil
	}

}

func generateName(component SSTableComponent, extension SSTableExtension) string {
	entries, err := os.ReadDir(SSTABLES_DIR)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	max := 0

	for _, entry := range entries {
		name := entry.Name()
		parts := strings.Split(name, "-")

		if len(parts) == 3 {
			genNum, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println(err)
				return ""
			}

			if genNum > max {
				max = genNum
			}
		}
	}

	newName := fmt.Sprintf(
		"sstable-%d-%s.%s",
		max+1,
		component,
		extension,
	)

	return newName
}

