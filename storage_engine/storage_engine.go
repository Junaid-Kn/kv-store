package main
import (
	// "errors"
	"os"
	"sync"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Engine interface {
    Put(Key, Value []byte) error
    Get(Key []byte) ([]byte, error)     // returns ErrNotFound if missing
    // Delete(Key []byte) error
    // Close() error                        // flush + close files cleanly
}


type KVStorage struct { 
	Mu sync.RWMutex
	MemTable SkipList
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
	defer s.Mu.Unlock()
	err := s.WriteToWAL(2, Key, Value);
	if err != nil{
		return err 
	}

	// write to memtable
	err = s.MemTable.Insert(Key, Value)
	if err != nil{
		fmt.Println("unable to insert key")
		return nil
	}

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
		return string(node.Value), nil
	}else{
		// read from disk 
		return "123", nil
	}

}


