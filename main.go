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

	fmt.Println()
	if failed > 0 {
		log.Fatalf("%d tombstone test(s) failed", failed)
	}
	fmt.Println("All tombstone tests passed.")
}
