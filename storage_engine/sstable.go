package storage_engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	LevelL0 = 0
	LevelL1 = 1
)

// metaMagic marks the SSTable .meta header: [magic u32][maxSeq u64][bloom...]
const metaMagic uint32 = 0x53535432

// SSTableID identifies one on-disk table: a flush (L0) or a compaction output (L1).
type SSTableID struct {
	Gen   int
	Level int
}

type sstRecord struct {
	key   []byte
	value []byte
	seq   uint64
	kind  uint8
}

func (s *KVStorage) tablePaths(id SSTableID) (data, index, meta string) {
	data = filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, DataComponent, SSTExtension))
	index = filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, IndexComponent, IndexExtension))
	meta = filepath.Join(s.SSTablesDir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, MetaComponent, MetaExtension))
	return
}

func parseSSTName(name string) (gen int, level int, ok bool) {
	// sstable-{gen}-{level}-{component}.{ext}
	if !strings.HasPrefix(name, "sstable-") {
		return 0, 0, false
	}
	rest := strings.TrimPrefix(name, "sstable-")
	parts := strings.SplitN(rest, "-", 3)
	if len(parts) != 3 {
		return 0, 0, false
	}
	gen, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	level, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return gen, level, true
}

func (s *KVStorage) recoverLiveTables() error {
	entries, err := os.ReadDir(s.SSTablesDir)
	if err != nil {
		return err
	}

	type counts struct {
		hasData  bool
		hasIndex bool
		hasMeta  bool
	}
	found := make(map[SSTableID]*counts)

	for _, entry := range entries {
		gen, level, ok := parseSSTName(entry.Name())
		if !ok {
			continue
		}
		if gen > s.nextGen {
			s.nextGen = gen
		}
		id := SSTableID{Gen: gen, Level: level}
		c := found[id]
		if c == nil {
			c = &counts{}
			found[id] = c
		}
		switch {
		case strings.HasSuffix(entry.Name(), "."+string(SSTExtension)):
			c.hasData = true
		case strings.HasSuffix(entry.Name(), "."+string(IndexExtension)):
			c.hasIndex = true
		case strings.HasSuffix(entry.Name(), "."+string(MetaExtension)):
			c.hasMeta = true
		}
	}

	s.liveTables = s.liveTables[:0]
	for id, c := range found {
		if c.hasData && c.hasIndex && c.hasMeta {
			s.liveTables = append(s.liveTables, id)
		}
	}
	sortForRead(s.liveTables)

	var maxSeq uint64
	for _, id := range s.liveTables {
		seq, err := s.tableMaxSeq(id)
		if err != nil {
			return err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	s.SequenceNum = maxSeq
	return nil
}

func (s *KVStorage) tableMaxSeq(id SSTableID) (uint64, error) {
	_, _, metaPath := s.tablePaths(id)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return 0, err
	}

	maxSeq, _, hasHeader := parseMeta(data)
	if hasHeader {
		return maxSeq, nil
	}

	return s.scanDataMaxSeq(id)
}

func (s *KVStorage) scanDataMaxSeq(id SSTableID) (uint64, error) {
	dataPath, _, _ := s.tablePaths(id)
	f, err := os.Open(dataPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var maxSeq uint64
	for {
		rec, err := readSSTRecord(f)
		if err == io.EOF {
			return maxSeq, nil
		}
		if err != nil {
			return 0, err
		}
		if rec.seq > maxSeq {
			maxSeq = rec.seq
		}
	}
}

func parseMeta(data []byte) (maxSeq uint64, bloom *BloomFilter, hasHeader bool) {
	if len(data) >= 12 && binary.LittleEndian.Uint32(data[:4]) == metaMagic {
		maxSeq = binary.LittleEndian.Uint64(data[4:12])
		return maxSeq, DeserializeBloomFilter(data[12:]), true
	}
	return 0, DeserializeBloomFilter(data), false
}

func sortForRead(tables []SSTableID) {
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Level != tables[j].Level {
			return tables[i].Level < tables[j].Level
		}
		return tables[i].Gen > tables[j].Gen
	})
}

func (s *KVStorage) allocateGen() int {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	s.nextGen++
	return s.nextGen
}

func (s *KVStorage) addLive(id SSTableID) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	s.liveTables = append(s.liveTables, id)
}

func (s *KVStorage) snapshotLive() []SSTableID {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	out := make([]SSTableID, len(s.liveTables))
	copy(out, s.liveTables)
	return out
}

func (s *KVStorage) l0CountLocked() int {
	n := 0
	for _, t := range s.liveTables {
		if t.Level == LevelL0 {
			n++
		}
	}
	return n
}

func (s *KVStorage) L0Count() int {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	return s.l0CountLocked()
}

func (s *KVStorage) SSTableCount() int {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	return len(s.liveTables)
}

func (s *KVStorage) LiveSSTables() []SSTableID {
	return s.snapshotLive()
}

func (s *KVStorage) installCompaction(inputs []SSTableID, output *SSTableID) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()

	drop := make(map[SSTableID]struct{}, len(inputs))
	for _, id := range inputs {
		drop[id] = struct{}{}
	}

	remaining := make([]SSTableID, 0, len(s.liveTables))
	for _, id := range s.liveTables {
		if _, ok := drop[id]; !ok {
			remaining = append(remaining, id)
		}
	}
	if output != nil {
		remaining = append(remaining, *output)
	}
	s.liveTables = remaining
}

func (s *KVStorage) deleteTableFiles(id SSTableID) {
	data, index, meta := s.tablePaths(id)
	_ = os.Remove(data)
	_ = os.Remove(index)
	_ = os.Remove(meta)
}

func readSSTRecord(r io.Reader) (sstRecord, error) {
	var keyLen uint32
	err := binary.Read(r, binary.LittleEndian, &keyLen)
	if err != nil {
		return sstRecord{}, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return sstRecord{}, err
	}

	var valueLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return sstRecord{}, err
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return sstRecord{}, err
	}

	var seq uint64
	if err := binary.Read(r, binary.LittleEndian, &seq); err != nil {
		return sstRecord{}, err
	}

	var kind uint8
	if err := binary.Read(r, binary.LittleEndian, &kind); err != nil {
		return sstRecord{}, err
	}

	return sstRecord{
		key:   key,
		value: value,
		seq:   seq,
		kind:  kind,
	}, nil
}

type sstWriter struct {
	id                 SSTableID
	dataFile           *os.File
	indexFile          *os.File
	metaFile           *os.File
	bloom              *BloomFilter
	currSize           int
	nextOffsetInterval int
	records            uint64
	maxSeq             uint64
}

func newSSTWriter(dir string, id SSTableID, estimatedKeys uint64) (*sstWriter, error) {
	if estimatedKeys == 0 {
		estimatedKeys = 1
	}

	dataPath := filepath.Join(dir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, DataComponent, SSTExtension))
	indexPath := filepath.Join(dir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, IndexComponent, IndexExtension))
	metaPath := filepath.Join(dir, fmt.Sprintf("sstable-%d-%d-%s.%s", id.Gen, id.Level, MetaComponent, MetaExtension))

	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		dataFile.Close()
		return nil, err
	}
	metaFile, err := os.OpenFile(metaPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, err
	}

	return &sstWriter{
		id:                 id,
		dataFile:           dataFile,
		indexFile:          indexFile,
		metaFile:           metaFile,
		bloom:              NewBloomFilter(estimatedKeys, 0.01),
		nextOffsetInterval: OFFSET_INTERVAL,
	}, nil
}

func (w *sstWriter) write(rec sstRecord) error {
	w.bloom.Add(rec.key)
	recordOffset := w.currSize

	if err := binary.Write(w.dataFile, binary.LittleEndian, uint32(len(rec.key))); err != nil {
		return err
	}
	if _, err := w.dataFile.Write(rec.key); err != nil {
		return err
	}
	if err := binary.Write(w.dataFile, binary.LittleEndian, uint32(len(rec.value))); err != nil {
		return err
	}
	if _, err := w.dataFile.Write(rec.value); err != nil {
		return err
	}
	if err := binary.Write(w.dataFile, binary.LittleEndian, rec.seq); err != nil {
		return err
	}
	if err := binary.Write(w.dataFile, binary.LittleEndian, rec.kind); err != nil {
		return err
	}

	w.currSize += 4 + len(rec.key) + 4 + len(rec.value) + 8 + 1
	w.records++
	if rec.seq > w.maxSeq {
		w.maxSeq = rec.seq
	}

	if w.currSize >= w.nextOffsetInterval {
		if err := binary.Write(w.indexFile, binary.LittleEndian, uint32(len(rec.key))); err != nil {
			return err
		}
		if _, err := w.indexFile.Write(rec.key); err != nil {
			return err
		}
		if err := binary.Write(w.indexFile, binary.LittleEndian, uint64(recordOffset)); err != nil {
			return err
		}
		w.nextOffsetInterval += OFFSET_INTERVAL
	}

	return nil
}

func (w *sstWriter) finish() error {
	var header [12]byte
	binary.LittleEndian.PutUint32(header[0:4], metaMagic)
	binary.LittleEndian.PutUint64(header[4:12], w.maxSeq)
	if _, err := w.metaFile.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.metaFile.Write(w.bloom.Serialize()); err != nil {
		return err
	}
	if err := w.dataFile.Close(); err != nil {
		return err
	}
	if err := w.indexFile.Close(); err != nil {
		return err
	}
	return w.metaFile.Close()
}

func (w *sstWriter) abort() {
	data := w.dataFile.Name()
	index := w.indexFile.Name()
	meta := w.metaFile.Name()
	w.dataFile.Close()
	w.indexFile.Close()
	w.metaFile.Close()
	_ = os.Remove(data)
	_ = os.Remove(index)
	_ = os.Remove(meta)
}
