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
	"path/filepath"
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

const MAX_SIZE_BEFORE_FLUSH = 32
const SSTABLES_DIR = "./sstables"
const OFFSET_INTERVAL = 4 * 1024

type Engine interface {
    Put(Key, Value []byte) error
    Get(Key []byte) ([]byte, error)     // returns ErrNotFound if missing
    // Delete(Key []byte) error
    // Close() error                        // flush + close files cleanly
}


type IndexEntry struct { 
	Key []byte
	ByteOffset uint64
}

type KVStorage struct { 
	Mu sync.RWMutex
	MemTable *SkipList
	SSTableIndex []IndexEntry
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



func (s * KVStorage) Put(Key,Value []byte) error {
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
		

	go func() {
		SSTFileName := generateName(DataComponent, SSTExtension)
		IndexFileName := generateName(IndexComponent, IndexExtension)
		
		f, err := os.OpenFile(
			SSTFileName,
			os.O_APPEND|os.O_CREATE|os.O_RDWR,
			0644,
		)
		if err != nil {
			fmt.Println("failed to open SSTable:", err)
			return
		}
		defer f.Close()


		idxFile, err := os.OpenFile(
			IndexFileName,
			os.O_APPEND|os.O_CREATE|os.O_RDWR,
			0644,
		)
		if err != nil {
			fmt.Println("failed to open index file :", err)
			return
		}

		defer idxFile.Close()

		curr := oldMemTable.HeadNode

		for curr.DownNode != nil {
			curr = curr.DownNode
		}

		curr = curr.NextNode
		currSize := 0
		nextOffsetInterval := OFFSET_INTERVAL

		for curr != nil {

			key := curr.Record.Key
			value := curr.Record.Value
			recordOffset := currSize
			err := binary.Write(
				f,
				binary.LittleEndian,
				uint32(len(key)),
			)
			if err != nil {
				fmt.Println("failed to write key length:", err)
				return
			}

			_, err = f.Write(key)
			if err != nil {
				fmt.Println("failed to write key:", err)
				return
			}

			err = binary.Write(
				f,
				binary.LittleEndian,
				uint32(len(value)),
			)
			if err != nil {
				fmt.Println("failed to write value length:", err)
				return
			}

			_, err = f.Write(value)
			if err != nil {
				fmt.Println("failed to write value:", err)
				return
			}

			currSize += 4 + len(key) + 4 + len(value)
			if currSize >= OFFSET_INTERVAL{
				idx := IndexEntry { 
					Key: append([]byte(nil), key...),
					ByteOffset: uint64(recordOffset),
				}
				// s.SSTableIndex = append(s.SSTableIndex, idx)
				
				nextOffsetInterval += OFFSET_INTERVAL

				// write to the indxFile in the following format
				// [KeyLen][Key][OffSet]
				err := binary.Write(
					idxFile,
					binary.LittleEndian,
					uint32(len(idx.Key)),
				)
				if err != nil {
					fmt.Println("failed to write key length:", err)
					return
				}

				_, err = f.Write(idx.Key)
				if err != nil {
					fmt.Println("failed to write key:", err)
					return
				}

				err = binary.Write(
					idxFile,
					binary.LittleEndian,
					uint64(recordOffset),
				)
				if err != nil {
					fmt.Println("failed to write key:", err)
					return
				}
				
			}

			curr = curr.NextNode
		}

		fmt.Println("Finished flushing:", SSTFileName)
		fmt.Println("Finished flushing:", IndexFileName)
	}()

		return nil
	}else{
		s.Mu.Unlock()
		return nil
	}
	 

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

	return filepath.Join(SSTABLES_DIR, newName)
}

