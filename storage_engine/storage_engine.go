package storage_engine

import (
	// "errors"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type SSTableComponent string
type SSTableExtension string

const (
	DataComponent  SSTableComponent = "data"
	IndexComponent SSTableComponent = "index"
	MetaComponent  SSTableComponent = "meta"
)

const (
	SSTExtension   SSTableExtension = "sst"
	IndexExtension SSTableExtension = "idx"
	MetaExtension  SSTableExtension = "meta"
)

const MAX_SIZE_BEFORE_FLUSH = 32 * 1024

// const SSTABLES_DIR = "./sstables"
const OFFSET_INTERVAL = 4 * 1024

// On-disk SSTable record kind. Every data record is:
// [keyLen u32][key][valLen u32][value][seq u64][kind u8]
const (
	KindPut    uint8 = 0
	KindDelete uint8 = 1
)

const (
	WALOpPut    uint8 = 2
	WALOpDelete uint8 = 3
)

var ErrNotFound = errors.New("key not found")

type Engine interface {
	Put(Key, Value []byte) error
	Get(Key []byte) ([]byte, error) // returns ErrNotFound if missing
	Delete(Key []byte) error
	// Close() error                        // flush + close files cleanly
}

type IndexEntry struct {
	Key        []byte
	ByteOffset uint64
}

type KVStorage struct {
	Mu           sync.RWMutex
	MemTable     *SkipList
	SSTableIndex []IndexEntry
	WALDir       string
	SSTablesDir  string
	DataDir      string
	BloomFilter  *BloomFilter
	SequenceNum  uint64
}

func NewKVStorage(dataDir string) (*KVStorage, error) {
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}

	s := &KVStorage{
		MemTable:    NewSkipList(16),
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

	if Op == WALOpPut {
		err = s.WriteToMTL(Key, Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *KVStorage) WriteToMTL(Key, Value []byte) error {
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

func (s *KVStorage) WriteToWAL(Op uint8, Key, Value []byte) error {
	f, err := OpenOrCreateFile(filepath.Join(s.WALDir, "wal.bin"))
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
	WALRecord := WALInput{
		TransactionId: uint64(curr_counter),
		Op:            Op, // INSERT,PUT, GET, DELETE
		Key:           Key,
		KeyLen:        uint16(len(Key)),
		Value:         Value,
		ValueLen:      uint32(len(Value)),
		SequenceNum:   uint64(s.SequenceNum),
		CheckSum:      0, // do checksum calc later
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

	binary.Write(f, binary.LittleEndian, WALRecord.SequenceNum)

	binary.Write(f, binary.LittleEndian, WALRecord.CheckSum)

	SaveCounter(int64(WALRecord.TransactionId))

	return nil
}

func (s *KVStorage) Put(Key, Value []byte) error {
	s.Mu.Lock()
	var err error

	s.SequenceNum++
	record := s.MemTable.GetRecord(Key)
	if record != nil {
		err = s.MemTable.Update(Key, Value, s.SequenceNum)
	} else {
		record := &Entry{
			Key:         append([]byte(nil), Key...),
			Value:       append([]byte(nil), Value...),
			SequenceNum: s.SequenceNum,
			Deleted:     false,
		}
		err = s.MemTable.Insert(record)
	}

	if err != nil {
		s.Mu.Unlock()
		return err
	}

	err = s.WriteRecord(WALOpPut, Key, Value)
	if err != nil {
		s.Mu.Unlock()
		return err
	}

	s.commitWrite()
	return nil
}

func (s *KVStorage) Delete(Key []byte) error {
	s.Mu.Lock()

	s.SequenceNum++
	record := s.MemTable.GetRecord(Key)
	var err error
	if record != nil {
		err = s.MemTable.SetTombstone(Key, s.SequenceNum)
	} else {
		tombstone := &Entry{
			Key:         append([]byte(nil), Key...),
			Value:       nil,
			SequenceNum: s.SequenceNum,
			Deleted:     true,
		}
		err = s.MemTable.Insert(tombstone)
	}

	if err != nil {
		s.Mu.Unlock()
		return err
	}

	err = s.WriteRecord(WALOpDelete, Key, nil)
	if err != nil {
		s.Mu.Unlock()
		return err
	}

	s.commitWrite()
	return nil
}

// commitWrite unlocks Mu. If the memtable is over the flush threshold,
// it freezes the table and flushes it in the background.
func (s *KVStorage) commitWrite() {
	if s.MemTable.Size >= MAX_SIZE_BEFORE_FLUSH {
		oldMemTable := s.MemTable
		s.MemTable = NewSkipList(16)
		s.Mu.Unlock()
		go s.flushMemTable(oldMemTable)
		return
	}
	s.Mu.Unlock()
}

func (s *KVStorage) flushMemTable(oldMemTable *SkipList) {
	entries, err := os.ReadDir(s.SSTablesDir)
	if err != nil {
		fmt.Println("failed to read SSTable directory:", err)
		return
	}
	gen := getMaxGenNumber(entries) + 1
	SSTFileName := filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%s.%s", gen, DataComponent, SSTExtension))
	IndexFileName := filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%s.%s", gen, IndexComponent, IndexExtension))
	MetaFileName := filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%s.%s", gen, MetaComponent, MetaExtension))

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
		fmt.Println("failed to open index file:", err)
		return
	}
	defer idxFile.Close()

	metaFile, err := os.OpenFile(
		MetaFileName,
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		fmt.Println("failed to open meta file:", err)
		return
	}
	defer metaFile.Close()

	// Find the bottom level of the skip list.
	curr := oldMemTable.HeadNode

	for curr.DownNode != nil {
		curr = curr.DownNode
	}

	// Skip the head/sentinel node.
	curr = curr.NextNode

	// create a new bloom filter for this table
	bloomFilter := NewBloomFilter(
		uint64(oldMemTable.Size),
		0.01, // 1% false-positive probability
	)

	currSize := 0
	nextOffsetInterval := OFFSET_INTERVAL

	for curr != nil {

		key := curr.Record.Key
		value := curr.Record.Value

		// Add the key to this SSTable's Bloom filter.
		bloomFilter.Add(key)

		recordOffset := currSize

		// -----------------------------------------------------
		// Write key length
		// -----------------------------------------------------

		err := binary.Write(
			f,
			binary.LittleEndian,
			uint32(len(key)),
		)
		if err != nil {
			fmt.Println("failed to write key length:", err)
			return
		}

		// -----------------------------------------------------
		// Write key
		// -----------------------------------------------------

		_, err = f.Write(key)
		if err != nil {
			fmt.Println("failed to write key:", err)
			return
		}

		// -----------------------------------------------------
		// Write value length
		// -----------------------------------------------------

		err = binary.Write(
			f,
			binary.LittleEndian,
			uint32(len(value)),
		)
		if err != nil {
			fmt.Println("failed to write value length:", err)
			return
		}

		// -----------------------------------------------------
		// Write value
		// -----------------------------------------------------

		_, err = f.Write(value)
		if err != nil {
			fmt.Println("failed to write value:", err)
			return
		}

		// -----------------------------------------------------
		// Write Sequence Number
		// -----------------------------------------------------

		err = binary.Write(
			f,
			binary.LittleEndian,
			uint64(curr.Record.SequenceNum),
		)
		if err != nil {
			fmt.Println("failed to write SequenceNum to disk", err)
			return
		}

		kind := KindPut
		if curr.Record.Deleted {
			kind = KindDelete
		}
		err = binary.Write(f, binary.LittleEndian, kind)
		if err != nil {
			fmt.Println("failed to write record kind to disk", err)
			return
		}
		// 4(keylen) + key_len + 4(val_len) + val_len + seq_num + kind
		currSize += 4 + len(key) + 4 + len(value) + 8 + 1

		// -----------------------------------------------------
		// Write sparse index entry
		// -----------------------------------------------------

		if currSize >= nextOffsetInterval {

			idx := IndexEntry{
				Key:        append([]byte(nil), key...),
				ByteOffset: uint64(recordOffset),
			}

			// [KeyLen][Key][Offset]

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
				fmt.Println("failed to write offset:", err)
				return
			}

			nextOffsetInterval += OFFSET_INTERVAL
		}

		curr = curr.NextNode
	}

	// ---------------------------------------------------------
	// Serialize Bloom Filter
	// ---------------------------------------------------------

	bloomData := bloomFilter.Serialize()
	if err != nil {
		fmt.Println("failed to serialize bloom filter:", err)
		return
	}

	_, err = metaFile.Write(bloomData)
	if err != nil {
		fmt.Println("failed to write bloom filter:", err)
		return
	}

	fmt.Println("Finished flushing:", SSTFileName)
	fmt.Println("Finished flushing:", IndexFileName)
	fmt.Println("Finished flushing:", MetaFileName)
}

func (s *KVStorage) Read(Key []byte) (string, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	record := s.MemTable.GetRecord(Key)
	if record != nil {
		if record.Deleted {
			return "", ErrNotFound
		}
		return string(record.Value), nil
	} else {
		// read from disk
		// Load SSTableIndex here first

		entries, err := os.ReadDir(s.SSTablesDir)
		if err != nil {
			fmt.Println(err)
			return "", err
		}

		max := getMaxGenNumber(entries)

		for max > 0 {

			// ---------------------------------------------------------
			// Load Bloom Filter
			// ---------------------------------------------------------

			metaFileName := fmt.Sprintf(
				"sstable-%d-%s.%s",
				max,
				MetaComponent,
				MetaExtension,
			)

			metaPath := filepath.Join(
				s.SSTablesDir,
				metaFileName,
			)

			metaFile, err := os.Open(metaPath)
			if err != nil {
				return "", err
			}

			err = s.LoadBloomFilter(metaFile)
			metaFile.Close()

			if err != nil {
				return "", err
			}

			// ---------------------------------------------------------
			// Check Bloom Filter
			// ---------------------------------------------------------

			if !s.BloomFilter.MayContain(Key) {
				// Key definitely does not exist in this SSTable.
				// Skip the index and data file completely.
				max--
				continue
			}

			// ---------------------------------------------------------
			// Load SSTable Index
			// ---------------------------------------------------------

			s.SSTableIndex = nil

			idxFile := fmt.Sprintf(
				"sstable-%d-%s.%s",
				max,
				IndexComponent,
				IndexExtension,
			)

			idxPath := filepath.Join(
				s.SSTablesDir,
				idxFile,
			)

			indexFile, err := os.Open(idxPath)
			if err != nil {
				return "", err
			}

			err = s.LoadSSTableIndex(indexFile)
			indexFile.Close()

			if err != nil {
				return "", err
			}

			// ---------------------------------------------------------
			// Search SSTable Index
			// ---------------------------------------------------------

			leftOffset, rightOffset, ok :=
				s.SearchSSTableIndex(Key)

			if !ok {
				max--
				continue
			}

			// ---------------------------------------------------------
			// Open Data File
			// ---------------------------------------------------------

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

			// ---------------------------------------------------------
			// Search Data File
			// ---------------------------------------------------------

			value, found, deleted, err := searchDataFile(
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
				if deleted {
					return "", ErrNotFound
				}
				return string(value), nil
			}

			// Not in this SSTable.
			// Try older SSTable.
			max--
		}

		return "", ErrNotFound

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
) ([]byte, bool, bool, error) {

	_, err := file.Seek(int64(leftOffset), io.SeekStart)
	if err != nil {
		return nil, false, false, err
	}

	for {
		currentPos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, false, false, err
		}

		if rightOffset != 0 && uint64(currentPos) >= rightOffset {
			return nil, false, false, nil
		}

		var keyLen uint32

		err = binary.Read(file, binary.LittleEndian, &keyLen)
		if err == io.EOF {
			return nil, false, false, nil
		}
		if err != nil {
			return nil, false, false, err
		}
		currentKey := make([]byte, keyLen)

		_, err = io.ReadFull(file, currentKey)
		if err != nil {
			return nil, false, false, err
		}
		var valueLen uint32

		err = binary.Read(file, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, false, false, err
		}

		value := make([]byte, valueLen)

		_, err = io.ReadFull(file, value)
		if err != nil {
			return nil, false, false, err
		}

		var sequenceNum uint64
		err = binary.Read(file, binary.LittleEndian, &sequenceNum)
		if err != nil {
			return nil, false, false, err
		}

		var kind uint8
		err = binary.Read(file, binary.LittleEndian, &kind)
		if err != nil {
			return nil, false, false, err
		}

		cmp := bytes.Compare(currentKey, key)

		if cmp == 0 {
			return value, true, kind == KindDelete, nil
		}

		if cmp > 0 {
			return nil, false, false, nil
		}
	}
}

func getMaxGenNumber(entries []os.DirEntry) int {
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
			Key:        key,
			ByteOffset: offset,
		}

		s.SSTableIndex = append(s.SSTableIndex, indexEntry)
	}
}

func (s *KVStorage) LoadBloomFilter(metaFile *os.File) error {
	data, err := io.ReadAll(metaFile)
	if err != nil {
		return err
	}

	bloomFilter := DeserializeBloomFilter(data)

	if bloomFilter == nil {
		return errors.New("failed to deserialize bloom filter")
	}

	s.BloomFilter = bloomFilter

	return nil
}
