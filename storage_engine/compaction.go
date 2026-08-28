package storage_engine

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

const COMPACTION_THRESHOLD = 4

type sstScanner struct {
	file *os.File
	head *sstRecord
}

func (s *KVStorage) maybeCompact() {
	if s.L0Count() < COMPACTION_THRESHOLD {
		return
	}
	s.compact()
}

func (s *KVStorage) compact() {
	if !s.compacting.CompareAndSwap(false, true) {
		return
	}
	defer s.compacting.Store(false)

	for s.L0Count() >= COMPACTION_THRESHOLD {
		inputs := s.snapshotLive()
		if len(inputs) < 2 {
			return
		}

		fmt.Printf("Compacting %d SSTables into L1...\n", len(inputs))
		if err := s.mergeTables(inputs); err != nil {
			fmt.Println("compaction failed:", err)
			return
		}
	}
}

func (s *KVStorage) mergeTables(inputs []SSTableID) error {
	scanners := make([]sstScanner, 0, len(inputs))
	defer func() {
		for i := range scanners {
			if scanners[i].file != nil {
				scanners[i].file.Close()
			}
		}
	}()

	estimatedKeys := uint64(1)
	for _, id := range inputs {
		dataPath, _, _ := s.tablePaths(id)
		f, err := os.Open(dataPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if n := uint64(info.Size() / 32); n > 0 {
			estimatedKeys += n
		}

		rec, err := readSSTRecord(f)
		if err == io.EOF {
			f.Close()
			continue
		}
		if err != nil {
			f.Close()
			return err
		}

		copyRec := rec
		scanners = append(scanners, sstScanner{file: f, head: &copyRec})
	}

	outID := SSTableID{
		Gen:   s.allocateGen(),
		Level: LevelL1,
	}

	writer, err := newSSTWriter(s.SSTablesDir, outID, estimatedKeys)
	if err != nil {
		return err
	}

	wroteAny := false
	for {
		minKey, bestIdx := minKeyIndex(scanners)
		if bestIdx < 0 {
			break
		}

		winner := cloneRecord(scanners[bestIdx].head)

		for i := range scanners {
			if scanners[i].head == nil || !bytes.Equal(scanners[i].head.key, minKey) {
				continue
			}
			if scanners[i].head.seq > winner.seq {
				winner = cloneRecord(scanners[i].head)
			}

			next, err := readSSTRecord(scanners[i].file)
			if err == io.EOF {
				scanners[i].head = nil
				continue
			}
			if err != nil {
				writer.abort()
				return err
			}
			copyNext := next
			scanners[i].head = &copyNext
		}

		// All live on-disk tables are inputs, so nothing older remains.
		// A tombstone can be dropped instead of rewritten.
		if winner.kind == KindDelete {
			continue
		}

		if err := writer.write(winner); err != nil {
			writer.abort()
			return err
		}
		wroteAny = true
	}

	if !wroteAny {
		writer.abort()
		s.installCompaction(inputs, nil)
		for _, id := range inputs {
			s.deleteTableFiles(id)
		}
		fmt.Println("Compaction complete: all keys were tombstones, dropped inputs")
		return nil
	}

	if err := writer.finish(); err != nil {
		writer.abort()
		return err
	}

	s.installCompaction(inputs, &outID)
	for _, id := range inputs {
		s.deleteTableFiles(id)
	}

	data, _, _ := s.tablePaths(outID)
	fmt.Println("Finished compacting into:", data)
	return nil
}

func minKeyIndex(scanners []sstScanner) ([]byte, int) {
	best := -1
	for i := range scanners {
		if scanners[i].head == nil {
			continue
		}
		if best < 0 || bytes.Compare(scanners[i].head.key, scanners[best].head.key) < 0 {
			best = i
		}
	}
	if best < 0 {
		return nil, -1
	}
	return scanners[best].head.key, best
}

func cloneRecord(rec *sstRecord) sstRecord {
	return sstRecord{
		key:   append([]byte(nil), rec.key...),
		value: append([]byte(nil), rec.value...),
		seq:   rec.seq,
		kind:  rec.kind,
	}
}
