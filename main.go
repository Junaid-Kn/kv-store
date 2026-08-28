package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Junaid-Kn/kv-store/storage_engine"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <data-directory>", os.Args[0])
	}
	dataDir, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	s, err := storage_engine.NewKVStorage(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	failed := 0
	pass := func(name string) {
		fmt.Printf("PASS  %s\n", name)
	}
	fail := func(name string, detail string) {
		failed++
		fmt.Printf("FAIL  %s: %s\n", name, detail)
	}

	expectValue := func(name string, key, want string) {
		got, err := s.Read([]byte(key))
		if err != nil {
			fail(name, fmt.Sprintf("Read(%q) error: %v", key, err))
			return
		}
		if got != want {
			fail(name, fmt.Sprintf("Read(%q) = %q, want %q", key, got, want))
			return
		}
		pass(name)
	}

	expectMissing := func(name string, key string) {
		got, err := s.Read([]byte(key))
		if err == nil {
			fail(name, fmt.Sprintf("Read(%q) = %q, want ErrNotFound", key, got))
			return
		}
		if !errors.Is(err, storage_engine.ErrNotFound) {
			fail(name, fmt.Sprintf("Read(%q) error = %v, want ErrNotFound", key, err))
			return
		}
		pass(name)
	}

	mustPut := func(key, value string) {
		if err := s.Put([]byte(key), []byte(value)); err != nil {
			log.Fatalf("Put(%q, %q): %v", key, value, err)
		}
	}

	mustDelete := func(key string) {
		if err := s.Delete([]byte(key)); err != nil {
			log.Fatalf("Delete(%q): %v", key, err)
		}
	}

	fillUntilFlush := func(prefix string) {
		before := s.MemTable.Size
		for i := 0; i < 3000; i++ {
			mustPut(
				fmt.Sprintf("%s-%05d", prefix, i),
				fmt.Sprintf("value-%05d", i),
			)
			if s.MemTable.Size < before && s.MemTable.Size < storage_engine.MAX_SIZE_BEFORE_FLUSH {
				break
			}
			before = s.MemTable.Size
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("=== TOMBSTONE TESTS ===")

	mustPut("user", "alice")
	expectValue("memtable put is readable", "user", "alice")

	mustDelete("user")
	expectMissing("memtable delete hides the key", "user")

	mustPut("user", "bob")
	expectValue("put after delete resurrects the key", "user", "bob")

	mustDelete("user")
	expectMissing("second delete hides the key again", "user")

	mustDelete("never-existed")
	expectMissing("delete of missing key is still not found", "never-existed")

	mustPut("keep-me", "alive")
	mustPut("drop-me", "gone")
	fmt.Println("flushing live values to SSTable...")
	fillUntilFlush("sst1")

	expectValue("flushed live key is readable from SSTable", "keep-me", "alive")
	expectValue("flushed key still readable before delete", "drop-me", "gone")

	mustDelete("drop-me")
	expectMissing("memtable tombstone hides SSTable value", "drop-me")
	expectValue("unrelated flushed key still readable", "keep-me", "alive")

	fmt.Println("flushing tombstone to a newer SSTable...")
	fillUntilFlush("sst2")

	expectMissing("SSTable tombstone hides older SSTable value", "drop-me")
	expectValue("unrelated key survives tombstone flush", "keep-me", "alive")

	mustPut("drop-me", "back")
	expectValue("put after SSTable tombstone resurrects the key", "drop-me", "back")

	fmt.Println("\n=== COMPACTION TESTS ===")

	mustPut("compact-keep", "v1")
	mustPut("compact-drop", "gone")
	mustDelete("compact-drop")

	for i := 0; i < storage_engine.COMPACTION_THRESHOLD; i++ {
		fmt.Printf("flushing L0 table %d (L0 count=%d)...\n", i+1, s.L0Count())
		fillUntilFlush(fmt.Sprintf("c%d", i))
	}
	time.Sleep(2 * time.Second)

	l0 := s.L0Count()
	total := s.SSTableCount()
	fmt.Printf("After compaction: L0=%d total SSTables=%d\n", l0, total)
	for _, t := range s.LiveSSTables() {
		fmt.Printf("  live table gen=%d level=%d\n", t.Gen, t.Level)
	}

	if l0 >= storage_engine.COMPACTION_THRESHOLD {
		fail("compaction reduces L0 below threshold", fmt.Sprintf("L0=%d", l0))
	} else {
		pass("compaction reduces L0 below threshold")
	}

	hasL1 := false
	for _, t := range s.LiveSSTables() {
		if t.Level == storage_engine.LevelL1 {
			hasL1 = true
			break
		}
	}
	if !hasL1 && total > 0 {
		fail("compaction produces an L1 table", "no L1 SSTable in live set")
	} else if hasL1 {
		pass("compaction produces an L1 table")
	}

	expectValue("key survives compaction", "compact-keep", "v1")
	expectMissing("tombstone dropped and key stays deleted after compaction", "compact-drop")
	expectValue("earlier live key still readable after compaction", "keep-me", "alive")
	expectValue("resurrected key still readable after compaction", "drop-me", "back")

	mustPut("compact-keep", "v2")
	expectValue("overwrite after compaction is readable", "compact-keep", "v2")

	fmt.Println("\n=== RESTART SEQUENCE TEST ===")
	seqAtShutdown := s.SequenceNum

	s2, err := storage_engine.NewKVStorage(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	if s2.SequenceNum == 0 && s2.SSTableCount() > 0 {
		fail("recovered SequenceNum from SSTables", "SequenceNum is 0 with live tables")
	} else {
		fmt.Printf("recovered SequenceNum=%d (process had %d)\n", s2.SequenceNum, seqAtShutdown)
		pass("recovered SequenceNum from SSTables")
	}

	if err := s2.Delete([]byte("keep-me")); err != nil {
		log.Fatalf("Delete keep-me on restart: %v", err)
	}
	got, err := s2.Read([]byte("keep-me"))
	if err == nil {
		fail("delete after restart hides SSTable key", fmt.Sprintf("Read(keep-me)=%q", got))
	} else if !errors.Is(err, storage_engine.ErrNotFound) {
		fail("delete after restart hides SSTable key", err.Error())
	} else {
		pass("delete after restart hides SSTable key")
	}

	fmt.Println()
	if failed > 0 {
		log.Fatalf("%d test(s) failed", failed)
	}
	fmt.Println("All tombstone and compaction tests passed.")
}
