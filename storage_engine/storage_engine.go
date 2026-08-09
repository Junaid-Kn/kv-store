package storage_engine
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
	"bytes"
	"io"
	"errors"
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

const MAX_SIZE_BEFORE_FLUSH = 32*1024
// const SSTABLES_DIR = "./sstables"
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
	WALDir string
	SSTablesDir string
	DataDir string
}


func NewKVStorage(dataDir string) (*KVStorage, error) {
    dataDir, err := filepath.Abs(dataDir)
    if err != nil {
        return nil, err
    }

    s := &KVStorage{
		MemTable: NewSkipList(16),
        DataDir:     dataDir,
        WALDir:      filepath.Join(dataDir, "wal"),
        SSTablesDir: filepath.Join(dataDir, "sstables"),
    }

    if err := os.MkdirAll(s.WALDir, 0755); err != nil {
        return nil, err
    }

    if err := os.MkdirAll(s.SSTablesDir, 0755); err != nil {
        return nil, err
    }

    return s, nil
}


func (s *KVStorage) WriteRecord(Op uint8, Key, Value []byte) error {
	err := s.WriteToWAL(Op, Key, Value)

	if err != nil {
		return err
	}

	if Op == 2{
		err = s.WriteToMTL(Key, Value)
		if err != nil {
			return err
		}
	}

	return nil 
}

func (s * KVStorage) WriteToMTL(Key, Value[]byte) error {
	f, err := os.OpenFile(
		"memtable_log.bin",
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		fmt.Println("failed to open MemTable Log:", err)
		return err
	}
	defer f.Close()

	err = binary.Write(
			f,
			binary.LittleEndian,
			uint32(len(Key)),
		)
		if err != nil {
			fmt.Println("failed to write key length:", err)
			return err
		}

		_, err = f.Write(Key)
		if err != nil {
			fmt.Println("failed to write key:", err)
			return err
		}

		err = binary.Write(
			f,
			binary.LittleEndian,
			uint32(len(Value)),
		)
		if err != nil {
			fmt.Println("failed to write value length:", err)
			return err
		}

		_, err = f.Write(Value)
		if err != nil {
			fmt.Println("failed to write value:", err)
			return err
		}


	
	return nil

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

	f.Write(WALRecord.Key)

	binary.Write(f, binary.LittleEndian, WALRecord.ValueLen)

	f.Write(WALRecord.Value)

	binary.Write(f, binary.LittleEndian, WALRecord.CheckSum)

	SaveCounter(int64(WALRecord.TransactionId))

	return nil
}



func (s * KVStorage) Put(Key,Value []byte) error {
	s.Mu.Lock()
	var err error
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

	err = s.WriteRecord(2, Key, Value);
	if err != nil{
		return err 
	}

	if s.MemTable.Size >= MAX_SIZE_BEFORE_FLUSH{
		// freeze current memTable and then flush it into SSTable
		newMemTable := NewSkipList(16)
		oldMemTable := s.MemTable
		s.MemTable = newMemTable
		s.Mu.Unlock()
		

	go func() {
		SSTFileName := GenerateName(DataComponent, SSTExtension, s.SSTablesDir)
		IndexFileName := GenerateName(IndexComponent, IndexExtension, s.SSTablesDir)
		
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

				_, err = idxFile.Write(idx.Key)
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
	err := s.WriteRecord(3, Key, []byte(""));
	if err != nil{
		return "", err 
	}
	_, node := s.MemTable.Get(Key)
	if node != nil  { 
		return string(node.Record.Value), nil
	}else{
		// read from disk
		// Load SSTableIndex here first
	
		entries, err := os.ReadDir(s.SSTablesDir)
		if err != nil {
			fmt.Println(err)
			return "", err
		}

		max := getMaxGenNumber(entries)

		for max > 0 {

			// Reset index for this SSTable
			s.SSTableIndex = nil

			idxFile := fmt.Sprintf(
				"sstable-%d-%s.%s",
				max,
				IndexComponent,
				IndexExtension,
			)

			idxPath := filepath.Join(s.SSTablesDir, idxFile)

			indexFile, err := os.Open(idxPath)
			if err != nil {
				return "", err
			}
			// Load SSTable Index into memory
			err = s.LoadSSTableIndex(indexFile)
			indexFile.Close()

			if err != nil {
				return "", err
			}

			leftOffset, rightOffset, ok :=
				s.SearchSSTableIndex(Key)

			if !ok {
				max--
				continue
			}

			// -------------------------
			// Open data file
			// -------------------------

			dataFileName := fmt.Sprintf(
				"sstable-%d-%s.%s",
				max,
				DataComponent,
				SSTExtension,
			)

			dataPath := filepath.Join(
				s.SSTablesDir,
				dataFileName,
			)

			dataFile, err := os.Open(dataPath)
			if err != nil {
				return "", err
			}

			// -------------------------
			// Search data
			// -------------------------

			value, found, err := searchDataFile(
				dataFile,
				Key,
				leftOffset,
				rightOffset,
			)

			dataFile.Close()

			if err != nil {
				return "", err
			}

			if found {
				return string(value), nil
			}

			// Not in this SSTable.
			// Try older one.
			max--
		}

		return "", errors.New("key not found")
	
	}


}

func (s *KVStorage) SearchSSTableIndex(Key []byte) (uint64, uint64, bool) {
    left := 0
    right := len(s.SSTableIndex) - 1
    candidate := -1

    // Find the largest index entry whose key <= Key
    for left <= right {
        mid := left + (right-left)/2

        cmp := bytes.Compare(s.SSTableIndex[mid].Key, Key)

        if cmp <= 0 {
            // This key is <= target.
            // It is a valid lower-bound candidate.
            candidate = mid
            left = mid + 1
        } else {
            // This key is > target.
            // Search to the left.
            right = mid - 1
        }
    }

    // Target is smaller than the first indexed key.
    if candidate == -1 {
        return 0, s.SSTableIndex[0].ByteOffset, true
    }

    leftOffset := s.SSTableIndex[candidate].ByteOffset

    // No next index entry means scan until EOF.
    if candidate+1 >= len(s.SSTableIndex) {
        return leftOffset, 0, true
    }

    rightOffset := s.SSTableIndex[candidate+1].ByteOffset

    return leftOffset, rightOffset, true
}

func searchDataFile(
    file *os.File,
    key []byte,
    leftOffset uint64,
    rightOffset uint64,
) ([]byte, bool, error) {

    _, err := file.Seek(int64(leftOffset), io.SeekStart)
    if err != nil {
        return nil, false, err
    }

    for {
		currentPos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, false, err
		}

		if rightOffset != 0 && uint64(currentPos) >= rightOffset {
			return nil, false, nil
		}

		var keyLen uint32

		err = binary.Read(file, binary.LittleEndian, &keyLen)
		if err != nil {
			return nil, false, err
		}
		currentKey := make([]byte, keyLen)

		_, err = io.ReadFull(file, currentKey)
		if err != nil {
			return nil, false, err
		}
		var valueLen uint32

		err = binary.Read(file, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, false, err
		}

		value := make([]byte, valueLen)

		_, err = io.ReadFull(file, value)
		if err != nil {
			return nil, false, err
		}

		cmp := bytes.Compare(currentKey, key)

		if cmp == 0 {
			return value, true, nil
		}

		if cmp > 0 {
			return nil, false, nil
		}
	}
}

func getMaxGenNumber(entries []os.DirEntry) int{
	max := 0
	for _, entry := range entries {
		name := entry.Name()
		parts := strings.Split(name, "-")

		if len(parts) == 3 {
			genNum, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println(err)
				return -1
			}

			if genNum > max {
				max = genNum
			}
		}
	}
	return max

}

func GenerateName(component SSTableComponent, extension SSTableExtension, dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	max := getMaxGenNumber(entries)
	if max == -1 {
		return "" 
	}


	newName := fmt.Sprintf(
		"sstable-%d-%s.%s",
		max+1,
		component,
		extension,
	)

	return filepath.Join(dir, newName)
}


func (s *KVStorage) LoadSSTableIndex(file *os.File) error {
    for {
        var keyLen uint32
        var offset uint64

        err := binary.Read(file, binary.LittleEndian, &keyLen)
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }

        key := make([]byte, keyLen)

        _, err = io.ReadFull(file, key)
        if err != nil {
            return err
        }

        err = binary.Read(file, binary.LittleEndian, &offset)
        if err != nil {
            return err
        }

        indexEntry := IndexEntry{
            Key:          key,
            ByteOffset: offset,
        }

        s.SSTableIndex = append(s.SSTableIndex, indexEntry)
    }
}

// func (s *KVStorage) LoadSSTableIndex(file *os.File) error {
// 	for {
// 		var keyLen uint32
// 		var offset uint64

// 		err := binary.Read(file, binary.LittleEndian, &keyLen)

// 		if err == io.EOF {
// 			return nil
// 		}

// 		if err != nil {
// 			fmt.Println("ERROR READING KEY LENGTH:", err)
// 			return err
// 		}

// 		fmt.Println("keyLen:", keyLen)

// 		key := make([]byte, keyLen)

// 		_, err = io.ReadFull(file, key)
// 		if err != nil {
// 			fmt.Println("ERROR READING KEY:", err)
// 			return err
// 		}

// 		fmt.Println("key:", string(key))

// 		err = binary.Read(file, binary.LittleEndian, &offset)
// 		if err != nil {
// 			fmt.Println("ERROR READING OFFSET:", err)
// 			return err
// 		}

// 		fmt.Println("offset:", offset)

// 		indexEntry := IndexEntry{
// 			Key:        key,
// 			ByteOffset: offset,
// 		}

// 		s.SSTableIndex = append(s.SSTableIndex, indexEntry)
// 	}
// }