package state

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// Embed only Store to verify compatibility with existing custom implementations.
type storeWithoutReplicationReads struct{ Store }

func TestReplicationReadsPreservePolicyAndOwnership(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			ctx := context.Background()
			policy := epochPolicy{id: "read-epoch"}
			var store Store
			dir := t.TempDir()
			if durable {
				s, err := NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local", Policy: policy})
				if err != nil {
					t.Fatal(err)
				}
				store = s
				defer func() { _ = store.(*DurableStore).Close() }()
			} else {
				s, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local", Policy: policy})
				if err != nil {
					t.Fatal(err)
				}
				store = s
			}
			for _, key := range []string{"d", "b", "a", "c"} {
				if _, err := store.Put(ctx, key, []byte("1")); err != nil {
					t.Fatal(err)
				}
			}
			e, _, err := store.Get(ctx, "b")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := store.CompareAndSet(ctx, "b", e.Identity(), Mutation{Value: []byte("2")}); err != nil || !ok {
				t.Fatalf("CAS: %v %v", ok, err)
			}
			if _, err := store.Delete(ctx, "c"); err != nil {
				t.Fatal(err)
			}
			if durable {
				if _, err := store.(*DurableStore).Compact(ctx); err != nil {
					t.Fatal(err)
				}
				if err := store.(*DurableStore).Close(); err != nil {
					t.Fatal(err)
				}
				store, err = NewDurableStore(DurableStoreConfig{Dir: dir, NodeID: "local", Policy: policy})
				if err != nil {
					t.Fatal(err)
				}
			}
			reader := store.(ReplicationReadStore)
			rows, err := reader.DigestRange(ctx, "b", "e")
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			for _, row := range rows {
				keys = append(keys, row.Key)
			}
			if !reflect.DeepEqual(keys, []string{"b", "c", "d"}) || !rows[1].Record.Deleted {
				t.Fatalf("range omitted boundary/tombstone: %+v", rows)
			}
			rows[0].Record.Metadata["epoch"][0] = '9'
			got, _, err := store.Get(ctx, "b")
			if err != nil || string(got.Value) != "2" {
				t.Fatalf("metadata aliases value: %q %v", got.Value, err)
			}
			indexed, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: store, Syncer: noopSyncer{}})
			if err != nil {
				t.Fatal(err)
			}
			fallback, err := NewGossipEngine(GossipConfig{NodeID: "local", Store: storeWithoutReplicationReads{store}, Syncer: noopSyncer{}})
			if err != nil {
				t.Fatal(err)
			}
			x, err := indexed.LocalDigest(ctx)
			if err != nil {
				t.Fatal(err)
			}
			y, err := fallback.LocalDigest(ctx)
			if err != nil || !reflect.DeepEqual(x, y) {
				t.Fatalf("digest policy mismatch: %v", err)
			}
			for _, start := range []string{"", "b", "c"} {
				req := PullRequest{ProtocolVersion: StateProtocolVersion, PolicyID: policy.ID(), StartKey: &start, EndKey: "e", Limit: 1, MaxBytes: 1024, Digest: map[string]DigestRecord{"d": x["d"]}}
				for {
					a, err := indexed.HandlePullResponse(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					b, err := fallback.HandlePullResponse(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					aj, _ := json.Marshal(a)
					bj, _ := json.Marshal(b)
					if string(aj) != string(bj) {
						t.Fatalf("diff differs: %s / %s", aj, bj)
					}
					if len(a.Entries) > 0 && len(a.Entries[0].Value) > 0 {
						a.Entries[0].Value[0] = '9'
						own, _, _ := store.Get(ctx, a.Entries[0].Key)
						if string(own.Value) == "9" {
							t.Fatal("response value aliases store")
						}
					}
					if !a.More {
						break
					}
					req.AfterKey = a.NextKey
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := reader.DigestRange(canceled, "", ""); err == nil {
				t.Fatal("canceled digest succeeded")
			}
			if _, err := reader.HasKey(canceled, "a"); err == nil {
				t.Fatal("canceled lookup succeeded")
			}
		})
	}
}

func TestReplicationReadsDuringMutation(t *testing.T) {
	ctx := context.Background()
	s, err := NewMemoryStore(MemoryStoreConfig{NodeID: "concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 3)
	for worker := 0; worker < 3; worker++ {
		go func(worker int) {
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key/%03d", i%40)
				switch worker {
				case 0:
					if _, err := s.Put(ctx, key, []byte("value")); err != nil {
						errs <- err
						return
					}
					if i%3 == 0 {
						if _, err := s.Delete(ctx, key); err != nil {
							errs <- err
							return
						}
					}
				case 1:
					rows, err := s.DigestRange(ctx, "key/010", "key/030")
					if err != nil {
						errs <- err
						return
					}
					for j, row := range rows {
						if row.Key < "key/010" || row.Key >= "key/030" || (j > 0 && rows[j-1].Key >= row.Key) {
							errs <- fmt.Errorf("unordered range at %s", row.Key)
							return
						}
					}
				case 2:
					if _, err := s.ChangesSinceBytes(ctx, 0, 1024); err != nil {
						errs <- err
						return
					}
					if _, err := s.DigestDiffPage(ctx, PullRequest{MaxBytes: 1024}, PullResponse{ProtocolVersion: StateProtocolVersion, PolicyID: s.Policy().ID()}); err != nil {
						errs <- err
						return
					}
				}
			}
			errs <- nil
		}(worker)
	}
	for i := 0; i < 3; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	indexed, err := s.DigestRange(ctx, "", "")
	if err != nil {
		t.Fatal(err)
	}
	all, err := s.ListEntries(ctx)
	if err != nil || len(indexed) != len(all) {
		t.Fatalf("lost indexed keys: %d/%d %v", len(indexed), len(all), err)
	}
}

func TestDigestPageCannotExposeUnsentValuesThroughSliceCapacity(t *testing.T) {
	ctx := context.Background()
	s, err := NewMemoryStore(MemoryStoreConfig{NodeID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c", "d"} {
		if _, err := s.Put(ctx, key, []byte("safe")); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.DigestDiffPage(ctx, PullRequest{Limit: 2, MaxBytes: 1024}, PullResponse{ProtocolVersion: StateProtocolVersion, PolicyID: s.Policy().ID()})
	if err != nil {
		t.Fatal(err)
	}
	// A caller can legally reslice up to capacity; no reachable value may alias
	// the store, including a candidate excluded by entry or byte limits.
	for _, entry := range page.Entries[:cap(page.Entries)] {
		if len(entry.Value) > 0 {
			entry.Value[0] = 'X'
		}
	}
	for _, key := range []string{"a", "b", "c", "d"} {
		entry, _, err := s.Get(ctx, key)
		if err != nil || string(entry.Value) != "safe" {
			t.Fatalf("response exposes store value for %s: %q %v", key, entry.Value, err)
		}
	}
}
