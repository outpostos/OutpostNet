package delivery

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestRandomCrashesAndTransportFailures10000(t *testing.T) {
	const total = 10000
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	random := rand.New(rand.NewSource(37))
	wire := NewMemoryTransport()
	var attempts, lostResponses, failures, outCrashes, inCrashes int
	transport := transportFunc(func(c context.Context, d string, b Batch) (BatchResult, error) {
		attempts++
		if random.Intn(5) == 0 {
			failures++
			return BatchResult{}, errors.New("injected disconnect")
		}
		r, e := wire.Send(c, d, b)
		if e == nil && random.Intn(5) == 0 {
			lostResponses++
			return BatchResult{}, errors.New("injected response loss after durable acceptance")
		}
		return r, e
	})
	oc := Config{Dir: t.TempDir(), NodeID: "a", Clock: clock, Transport: transport}
	ic := Config{Dir: t.TempDir(), NodeID: "b", Clock: clock, Transport: transport}
	o := mustOut(t, oc)
	in := mustIn(t, ic)
	wire.Register("a", nil, o)
	wire.Register("b", in, nil)
	acks := make(map[MessageID]int, total)
	sent := make(map[MessageID]bool, total)
	for start := 0; start < total; start += 100 {
		for n := start; n < start+100; n++ {
			e := message(now)
			e.Payload = []byte(fmt.Sprintf("message-%d", n))
			e.WantHandoffReceipt = true
			id, err := o.Send(ctx, e)
			if err != nil {
				t.Fatal(err)
			}
			sent[id] = true
		}
		for round := 0; round < 100; round++ {
			now = now.Add(time.Second)
			if random.Intn(20) == 0 {
				if err := o.Close(); err != nil {
					t.Fatal(err)
				}
				o = mustOut(t, oc)
				wire.Register("a", nil, o)
				outCrashes++
			}
			_ = o.Flush(ctx) // injected transport failures must leave pending data
			if random.Intn(20) == 0 {
				if err := in.Close(); err != nil {
					t.Fatal(err)
				}
				in = mustIn(t, ic)
				wire.Register("b", in, nil)
				inCrashes++
			}
			for {
				readCtx, stop := context.WithTimeout(ctx, time.Millisecond)
				d, err := in.Receive(readCtx, "m")
				stop()
				if errors.Is(err, context.DeadlineExceeded) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if !sent[d.MessageID] || !now.Before(d.ExpiryTime) {
					t.Fatalf("unknown or expired delivery %+v", d)
				}
				// Consumer never crashes between processing and Ack in this test.
				if err = in.Ack(ctx, d.MessageID); err != nil {
					t.Fatal(err)
				}
				acks[d.MessageID]++
				if acks[d.MessageID] != 1 {
					t.Fatalf("repeated consumer Ack: %+v", d.MessageID)
				}
			}
			_ = in.Flush(ctx)
			complete := true
			for id := range sent {
				status, err := o.Status(id)
				if err != nil {
					t.Fatal(err)
				}
				if status != HandedOff {
					complete = false
					break
				}
			}
			if complete {
				break
			}
			if round == 99 {
				t.Fatal("did not converge")
			}
		}
		if (start+100)%1000 == 0 {
			t.Logf("progress: %d/%d uniquely Acked", len(acks), total)
		}
	}
	if len(acks) != total {
		t.Fatalf("Acked %d of %d", len(acks), total)
	}
	if failures == 0 || lostResponses == 0 || outCrashes == 0 || inCrashes == 0 {
		t.Fatal("fault scenario did not exercise all failures")
	}
	t.Logf("sent=%d unique_acks=%d attempts=%d disconnects=%d lost_responses=%d sender_restarts=%d receiver_restarts=%d", total, len(acks), attempts, failures, lostResponses, outCrashes, inCrashes)
}
