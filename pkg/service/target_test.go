package service_test

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/service"
)

// recordingNet is a NetStreams that records which transport path was asked
// for and hands back one end of an in-memory pipe.
type recordingNet struct {
	calls []string
}

func (r *recordingNet) OpenPeerStream(_ context.Context, nodeID, name string) (io.ReadWriteCloser, error) {
	r.calls = append(r.calls, "peer:"+nodeID+":"+name)
	a, b := net.Pipe()
	b.Close()
	return a, nil
}

func (r *recordingNet) OpenRegionStream(_ context.Context, region, name string) (io.ReadWriteCloser, error) {
	r.calls = append(r.calls, "region:"+region+":"+name)
	a, b := net.Pipe()
	b.Close()
	return a, nil
}

func TestNetOpenerMapsTargetsToTransportPaths(t *testing.T) {
	rn := &recordingNet{}
	op := service.NetOpener(rn)
	for _, tgt := range []service.Target{
		{Service: "echo", NodeID: "node-b"},
		{Service: "db", Region: "east"},
		{Service: "frp"},
	} {
		s, err := op.OpenStream(context.Background(), tgt)
		if err != nil {
			t.Fatalf("OpenStream(%v): %v", tgt, err)
		}
		s.Close()
	}
	want := []string{"peer:node-b:echo", "region:east:db", "region::frp"}
	if strings.Join(rn.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v", rn.calls, want)
	}
}

func TestNetOpenerRejectsInvalidTargetBeforeTransport(t *testing.T) {
	rn := &recordingNet{}
	op := service.NetOpener(rn)
	for _, tgt := range []service.Target{
		{NodeID: "node-b"},
		{Service: strings.Repeat("x", 256), NodeID: "node-b"},
		{Service: "echo", NodeID: "node-b", Region: "east"},
	} {
		if _, err := op.OpenStream(context.Background(), tgt); err == nil {
			t.Errorf("OpenStream(%v) succeeded, want validation error", tgt)
		}
	}
	if len(rn.calls) != 0 {
		t.Fatalf("transport reached for invalid targets: %v", rn.calls)
	}
}
