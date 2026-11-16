package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestLocalEntryLimitRejectsWritesWithoutChangingState(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ctx := context.Background()
			s := pagingStore(t, durable, "source")
			original, err := s.Put(ctx, "key", []byte("small"))
			if err != nil {
				t.Fatal(err)
			}
			var notifications int
			stop := s.Observe(func(Entry) { notifications++ })
			defer stop()
			large := bytes.Repeat([]byte("x"), 512<<10)
			if _, err := s.Put(ctx, "new", large); !errors.Is(err, ErrEntryTooLarge) {
				t.Fatalf("Put oversized: %v", err)
			}
			if _, swapped, err := s.CompareAndSet(ctx, "key", original.Identity(), Mutation{Value: large}); swapped || !errors.Is(err, ErrEntryTooLarge) {
				t.Fatalf("CAS oversized: swapped=%v err=%v", swapped, err)
			}
			if notifications != 0 {
				t.Fatalf("rejected writes notified %d observers", notifications)
			}
			entries, err := s.ListEntries(ctx)
			if err != nil || len(entries) != 1 || entries[0].Identity() != original.Identity() {
				t.Fatalf("rejected writes changed state: %d entries, %v", len(entries), err)
			}
		})
	}
}

func TestEntryLimitConfigurationAndEncodedSize(t *testing.T) {
	ctx := context.Background()
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			var s Store
			var err error
			if durable {
				d, openErr := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: "source", MaxEntryBytes: 1024})
				err = openErr
				if d != nil {
					s = d
					defer d.Close()
				}
			} else {
				s, err = NewMemoryStore(MemoryStoreConfig{NodeID: "source", MaxEntryBytes: 1024})
			}
			if err != nil {
				t.Fatal(err)
			}
			// Base64 alone expands 768 raw bytes to 1024; metadata must count too.
			if _, err := s.Put(ctx, "key", make([]byte, 768)); !errors.Is(err, ErrEntryTooLarge) {
				t.Fatalf("encoded size was ignored: %v", err)
			}
			if _, err := s.Put(ctx, string(bytes.Repeat([]byte("<"), 200)), nil); !errors.Is(err, ErrEntryTooLarge) {
				t.Fatalf("escaped key size was ignored: %v", err)
			}
			if _, err := s.Put(ctx, "small", make([]byte, 128)); err != nil {
				t.Fatalf("small write refused: %v", err)
			}
		})
	}
	if _, err := NewMemoryStore(MemoryStoreConfig{NodeID: "source", MaxEntryBytes: -1}); err == nil {
		t.Fatal("negative memory limit accepted")
	}
	if _, err := NewDurableStore(DurableStoreConfig{Dir: t.TempDir(), NodeID: "source", MaxEntryBytes: -1}); err == nil {
		t.Fatal("negative durable limit accepted")
	}
}
