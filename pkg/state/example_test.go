package state

import (
	"context"
	"fmt"
)

func ExampleMemoryStore_CompareAndSet() {
	ctx := context.Background()
	store, _ := NewMemoryStore(MemoryStoreConfig{
		NodeID:        "node-a",
		PhysicalClock: PhysicalClockFunc(func() int64 { return 1_000 }),
	})

	created, _ := store.Put(ctx, "opaque/entity-1", []byte("v1"))
	updated, swapped, _ := store.CompareAndSet(ctx, "opaque/entity-1", created.Identity(), Mutation{Value: []byte("v2")})
	got, ok, _ := store.Get(ctx, "opaque/entity-1")

	fmt.Println(swapped, ok, updated.ChangeID != created.ChangeID, string(got.Value))
	// Output: true true true v2
}
