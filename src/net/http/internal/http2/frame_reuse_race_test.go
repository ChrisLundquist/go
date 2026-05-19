// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

// The tests in this file exercise the Framer.SetReuseFrames contract
// from a concurrency angle. The reuse contract says the *Frame returned
// by ReadFrame, and any slice aliasing the Framer's read buffer
// reachable from that frame, are only valid until the next ReadFrame
// call. A retention bug under reuse manifests as one goroutine reading
// a retained slice while another (the reader) writes into the same
// backing memory on the next ReadFrame.
//
// These tests model the server's readFrames goroutine pattern (one
// reader goroutine, one consumer goroutine separated by a gate
// channel) and aim to be effective under "go test -race":
//
//   - TestFrameReuseRaceCorrect: faithful gated handoff. Must NOT race
//     under -race, regardless of how many frames are read. This is a
//     positive control that asserts the gated pattern is safe with
//     reuse on.
//
//   - TestFrameReuseRaceAdversarial: deliberately leaks a slice past
//     the gate. Must race under -race. Guarded behind the
//     H2_REUSE_RACE_NEGATIVE=1 env var so normal `go test` and CI
//     stay green; the assertion of the negative case is the race
//     detector itself.
//
// A companion end-to-end stress test exists in
// frame_reuse_e2e_test.go (package http2_test) which drives the real
// net/http HTTP/2 server and Transport under -race. The synthetic
// tests in this file cannot reach the production process* code paths;
// the e2e test does.

// preEncodeReuseRaceStream encodes a long sequence of frames
// exercising every frame type extended by SetReuseFrames: DATA,
// WINDOW_UPDATE, HEADERS, and HEADERS+CONTINUATION (so the meta
// header path runs). All HEADERS/DATA payloads are the same size so
// the framer's readBuf does not reallocate between frames; the
// retained slice in the adversarial subtest therefore continues to
// alias the same backing array that the next ReadFrame writes into.
func preEncodeReuseRaceStream(tb testing.TB, iters int) []byte {
	tb.Helper()
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	// Encode HPACK fields once; reuse to keep payload sizes stable.
	hdrBlock := encodeHeaderRaw(tb,
		":method", "GET",
		":path", "/",
		":scheme", "http",
		":authority", "example.com",
	)
	// Split hdrBlock so we can emit HEADERS+CONTINUATION for the
	// meta path.
	half := len(hdrBlock) / 2
	if half == 0 {
		half = 1
	}
	// Use a fixed-length data payload so the readBuf size stays
	// constant once it grows on the first read.
	dataPayload := bytes.Repeat([]byte{0xab}, 64)

	for i := 0; i < iters; i++ {
		streamID := uint32(2*i + 1)
		// DATA
		if err := fr.WriteData(streamID, false, dataPayload); err != nil {
			tb.Fatal(err)
		}
		// WINDOW_UPDATE
		if err := fr.WriteWindowUpdate(streamID, uint32(1+i%4096)); err != nil {
			tb.Fatal(err)
		}
		// HEADERS (single-frame, EndHeaders=true)
		if err := fr.WriteHeaders(HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: hdrBlock,
			EndHeaders:    true,
		}); err != nil {
			tb.Fatal(err)
		}
		// HEADERS + CONTINUATION (drives the meta-headers path)
		if err := fr.WriteHeaders(HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: hdrBlock[:half],
			EndHeaders:    false,
		}); err != nil {
			tb.Fatal(err)
		}
		if err := fr.WriteContinuation(streamID, true, hdrBlock[half:]); err != nil {
			tb.Fatal(err)
		}
	}
	return buf.Bytes()
}

// runReadFramesGoroutine launches a goroutine that mimics
// serverConn.readFrames: reads a frame, sends it to ch, waits on the
// returned gate before reading the next. Returns the gate channel and
// a done channel that closes once the reader goroutine exits.
type framedRead struct {
	frame Frame
	err   error
	// done must be called once the consumer no longer retains frame
	// or any slice aliasing the framer's read buffer.
	done chan<- struct{}
}

func runReadFramesGoroutine(tb testing.TB, fr *Framer, ch chan<- framedRead) (exited <-chan struct{}) {
	tb.Helper()
	exitc := make(chan struct{})
	go func() {
		defer close(exitc)
		for {
			gate := make(chan struct{})
			f, err := fr.ReadFrame()
			ch <- framedRead{frame: f, err: err, done: gate}
			// Wait for the consumer to signal done before reading
			// again. This re-creates the readFrames gate.
			<-gate
			if err != nil {
				return
			}
		}
	}()
	return exitc
}

// consumeSliceWork is a noinline helper that reads every byte of b
// once. Its only purpose is to ensure the race detector observes a
// read of the memory backing b. If a concurrent goroutine writes the
// same memory without a happens-before edge, -race will fire here.
//
//go:noinline
func consumeSliceWork(b []byte) byte {
	var x byte
	for _, v := range b {
		x ^= v
	}
	return x
}

// consumeHeaderFieldWork reads the name+value strings of every
// HeaderField. The string bytes themselves do not alias the framer's
// read buffer — hpack.decodeString allocates fresh strings via
// string(u.b) or bytes.Buffer.String() — so iterating Name/Value
// post-gate is not, in itself, a data race. The reuse-contract
// violation captured by the adversarial test below is retention of
// the Fields slice *header* (whose backing array is reachable from
// the cached *MetaHeadersFrame) past the next ReadFrame; the race
// detector cannot observe that for MetaHeadersFrame because hpack's
// string copies make the byte memory independent of the framer's
// read buffer. The function is kept for symmetry with consumeSliceWork
// and to document the contract for the MetaHeadersFrame case.
//
//go:noinline
func consumeHeaderFieldWork(fields []hpack.HeaderField) byte {
	var x byte
	for _, hf := range fields {
		for i := 0; i < len(hf.Name); i++ {
			x ^= hf.Name[i]
		}
		for i := 0; i < len(hf.Value); i++ {
			x ^= hf.Value[i]
		}
	}
	return x
}

// TestFrameReuseRaceCorrect runs the full reader-consumer dance with
// SetReuseFrames enabled, consuming every frame's payload before
// signaling the gate. Under -race, this MUST NOT fire.
//
// What this catches: a reuse implementation that mutates the cached
// frame or its backing buffer before the consumer signals done (for
// example, by parsing the next frame eagerly on a background
// goroutine or by reusing the buffer for some other purpose).
func TestFrameReuseRaceCorrect(t *testing.T) {
	const iters = 200

	encoded := preEncodeReuseRaceStream(t, iters)
	r := bytes.NewReader(encoded)
	fr := NewFramer(io.Discard, r)
	fr.SetReuseFrames()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	ch := make(chan framedRead)
	exitc := runReadFramesGoroutine(t, fr, ch)

	// sink lets the compiler keep the work observable.
	var sink atomic.Uint64

	for {
		select {
		case res := <-ch:
			if res.err == io.EOF {
				close(res.done)
				goto drain
			}
			if res.err != nil {
				t.Fatalf("unexpected ReadFrame error: %v", res.err)
			}
			// Consume the slice/fields synchronously. Once we
			// signal done, we forget the frame.
			switch f := res.frame.(type) {
			case *DataFrame:
				sink.Add(uint64(consumeSliceWork(f.Data())))
			case *HeadersFrame:
				sink.Add(uint64(consumeSliceWork(f.HeaderBlockFragment())))
			case *MetaHeadersFrame:
				sink.Add(uint64(consumeHeaderFieldWork(f.Fields)))
			case *WindowUpdateFrame:
				sink.Add(uint64(f.Increment))
			default:
				t.Fatalf("unexpected frame type %T", res.frame)
			}
			close(res.done) // gate: reader may proceed.
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for next frame")
		}
	}
drain:
	<-exitc
	t.Logf("consumed %d frames, sink=%d", iters*4, sink.Load())
}

// TestFrameReuseRaceAdversarial deliberately violates the reuse
// contract: the consumer hands a retained slice to a sidecar
// goroutine that keeps reading it indefinitely, while the gate is
// signaled immediately so the reader proceeds to overwrite the same
// memory. Under -race, this MUST fire.
//
// It is guarded behind H2_REUSE_RACE_NEGATIVE=1 because it is a
// negative control: failure (i.e., no race detected) is what we want
// to be loud about during development, but a passing test on stock
// CI is uninteresting. Run it as:
//
//	H2_REUSE_RACE_NEGATIVE=1 go test -race -run TestFrameReuseRaceAdversarial
//
// What it would catch in production code: any handler/code path that
// holds onto HeaderBlockFragment, Data, or hpack.HeaderField name/value
// strings past the readMore call.
func TestFrameReuseRaceAdversarial(t *testing.T) {
	if os.Getenv("H2_REUSE_RACE_NEGATIVE") != "1" {
		t.Skip("skipping adversarial negative-control test; " +
			"set H2_REUSE_RACE_NEGATIVE=1 to run under -race")
	}
	// 50 outer iterations -> 4*50 = 200 frames. That is far more
	// than enough scheduler interleavings for the race detector to
	// observe at least one concurrent read/write conflict.
	const iters = 50
	const maxLiveAttackers = 8 // cap concurrent attacker goroutines

	encoded := preEncodeReuseRaceStream(t, iters)
	r := bytes.NewReader(encoded)
	fr := NewFramer(io.Discard, r)
	fr.SetReuseFrames()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	ch := make(chan framedRead)
	exitc := runReadFramesGoroutine(t, fr, ch)

	// stopAttackers is closed when the test is winding down to let
	// any retained-slice readers exit.
	stopAttackers := make(chan struct{})
	var attackers sync.WaitGroup
	// Token bucket bounds the number of attacker goroutines alive
	// at any time; without it the race detector slows to a crawl as
	// many hundreds of goroutines all hammer the read buffer.
	tokens := make(chan struct{}, maxLiveAttackers)

	// retainedRefs accumulates references we deliberately leak past
	// the gate. Holding them in the test goroutine keeps them
	// reachable for the GC's view, but the race detector still
	// catches read/write conflicts on the underlying memory.
	type retainedSlice struct {
		b []byte
	}
	type retainedFields struct {
		hf []hpack.HeaderField
	}
	var refs []any

	startAttacker := func(read func()) {
		// Block briefly if too many attackers are already running.
		// This will bound the runtime overhead without weakening the
		// race detector signal (each retained reference still gets
		// a goroutine that overlaps the next ReadFrame).
		select {
		case tokens <- struct{}{}:
		case <-stopAttackers:
			return
		}
		attackers.Add(1)
		go func() {
			defer attackers.Done()
			defer func() { <-tokens }()
			for i := 0; i < 200; i++ {
				select {
				case <-stopAttackers:
					return
				default:
				}
				read()
			}
		}()
	}

	var sink atomic.Uint64

	for {
		select {
		case res := <-ch:
			if res.err == io.EOF {
				close(res.done)
				goto drain
			}
			if res.err != nil {
				t.Fatalf("unexpected ReadFrame error: %v", res.err)
			}
			switch f := res.frame.(type) {
			case *DataFrame:
				retained := retainedSlice{b: f.Data()}
				refs = append(refs, retained)
				startAttacker(func() {
					sink.Add(uint64(consumeSliceWork(retained.b)))
				})
			case *HeadersFrame:
				retained := retainedSlice{b: f.HeaderBlockFragment()}
				refs = append(refs, retained)
				startAttacker(func() {
					sink.Add(uint64(consumeSliceWork(retained.b)))
				})
			case *MetaHeadersFrame:
				// Retaining the Fields slice header past the gate
				// violates the reuse contract. The race detector
				// will NOT fire for this frame type because hpack
				// always allocates independent strings (see
				// consumeHeaderFieldWork); the byte memory backing
				// Name/Value is unrelated to the framer's read
				// buffer. Included here to document contract intent;
				// the DataFrame and HeadersFrame cases above are the
				// ones that actually trigger -race.
				retained := retainedFields{hf: f.Fields}
				refs = append(refs, retained)
				startAttacker(func() {
					sink.Add(uint64(consumeHeaderFieldWork(retained.hf)))
				})
			case *WindowUpdateFrame:
				// No slice to retain on WindowUpdateFrame, just
				// signal done.
			default:
				t.Fatalf("unexpected frame type %T", res.frame)
			}
			// Adversarial: signal done immediately, even though
			// we still have outstanding readers of the frame's
			// memory.
			close(res.done)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for next frame")
		}
	}
drain:
	close(stopAttackers)
	attackers.Wait()
	<-exitc
	// Force refs to outlive the loop above so the compiler does
	// not eliminate the retentions.
	if len(refs) == 0 {
		t.Fatalf("expected retained references, got 0")
	}
	t.Logf("retained %d references, sink=%d", len(refs), sink.Load())
}
