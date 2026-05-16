// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/net/http2/hpack"
)

func testFramer() (*Framer, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	return NewFramer(buf, buf), buf
}

func TestFrameSizes(t *testing.T) {
	// Catch people rearranging the FrameHeader fields.
	if got, want := int(unsafe.Sizeof(FrameHeader{})), 12; got != want {
		t.Errorf("FrameHeader size = %d; want %d", got, want)
	}
}

func TestFrameTypeString(t *testing.T) {
	tests := []struct {
		ft   FrameType
		want string
	}{
		{FrameData, "DATA"},
		{FramePing, "PING"},
		{FrameGoAway, "GOAWAY"},
		{0x20, "UNKNOWN_FRAME_TYPE_32"},
	}

	for i, tt := range tests {
		got := tt.ft.String()
		if got != tt.want {
			t.Errorf("%d. String(FrameType %d) = %q; want %q", i, int(tt.ft), got, tt.want)
		}
	}
}

func TestWriteRST(t *testing.T) {
	fr, buf := testFramer()
	var streamID uint32 = 1<<24 + 2<<16 + 3<<8 + 4
	var errCode uint32 = 7<<24 + 6<<16 + 5<<8 + 4
	fr.WriteRSTStream(streamID, ErrCode(errCode))
	const wantEnc = "\x00\x00\x04\x03\x00\x01\x02\x03\x04\x07\x06\x05\x04"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	want := &RSTStreamFrame{
		FrameHeader: FrameHeader{
			valid:    true,
			Type:     0x3,
			Flags:    0x0,
			Length:   0x4,
			StreamID: 0x1020304,
		},
		ErrCode: 0x7060504,
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("parsed back %#v; want %#v", f, want)
	}
}

func TestWriteData(t *testing.T) {
	fr, buf := testFramer()
	var streamID uint32 = 1<<24 + 2<<16 + 3<<8 + 4
	data := []byte("ABC")
	fr.WriteData(streamID, true, data)
	const wantEnc = "\x00\x00\x03\x00\x01\x01\x02\x03\x04ABC"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	df, ok := f.(*DataFrame)
	if !ok {
		t.Fatalf("got %T; want *DataFrame", f)
	}
	if !bytes.Equal(df.Data(), data) {
		t.Errorf("got %q; want %q", df.Data(), data)
	}
	if f.Header().Flags&1 == 0 {
		t.Errorf("didn't see END_STREAM flag")
	}
}

func TestWriteDataPadded(t *testing.T) {
	tests := [...]struct {
		streamID   uint32
		endStream  bool
		data       []byte
		pad        []byte
		wantHeader FrameHeader
	}{
		// Unpadded:
		0: {
			streamID:  1,
			endStream: true,
			data:      []byte("foo"),
			pad:       nil,
			wantHeader: FrameHeader{
				Type:     FrameData,
				Flags:    FlagDataEndStream,
				Length:   3,
				StreamID: 1,
			},
		},

		// Padded bit set, but no padding:
		1: {
			streamID:  1,
			endStream: true,
			data:      []byte("foo"),
			pad:       []byte{},
			wantHeader: FrameHeader{
				Type:     FrameData,
				Flags:    FlagDataEndStream | FlagDataPadded,
				Length:   4,
				StreamID: 1,
			},
		},

		// Padded bit set, with padding:
		2: {
			streamID:  1,
			endStream: false,
			data:      []byte("foo"),
			pad:       []byte{0, 0, 0},
			wantHeader: FrameHeader{
				Type:     FrameData,
				Flags:    FlagDataPadded,
				Length:   7,
				StreamID: 1,
			},
		},
	}
	for i, tt := range tests {
		fr, _ := testFramer()
		fr.WriteDataPadded(tt.streamID, tt.endStream, tt.data, tt.pad)
		f, err := fr.ReadFrame()
		if err != nil {
			t.Errorf("%d. ReadFrame: %v", i, err)
			continue
		}
		got := f.Header()
		tt.wantHeader.valid = true
		if !got.Equal(tt.wantHeader) {
			t.Errorf("%d. read %+v; want %+v", i, got, tt.wantHeader)
			continue
		}
		df := f.(*DataFrame)
		if !bytes.Equal(df.Data(), tt.data) {
			t.Errorf("%d. got %q; want %q", i, df.Data(), tt.data)
		}
	}
}

func (fh FrameHeader) Equal(b FrameHeader) bool {
	return fh.valid == b.valid &&
		fh.Type == b.Type &&
		fh.Flags == b.Flags &&
		fh.Length == b.Length &&
		fh.StreamID == b.StreamID
}

func TestWriteHeaders(t *testing.T) {
	tests := []struct {
		name      string
		p         HeadersFrameParam
		wantEnc   string
		wantFrame *HeadersFrame
	}{
		{
			"basic",
			HeadersFrameParam{
				StreamID:      42,
				BlockFragment: []byte("abc"),
				Priority:      PriorityParam{},
			},
			"\x00\x00\x03\x01\x00\x00\x00\x00*abc",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Length:   uint32(len("abc")),
				},
				Priority:      PriorityParam{},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"basic + end flags",
			HeadersFrameParam{
				StreamID:      42,
				BlockFragment: []byte("abc"),
				EndStream:     true,
				EndHeaders:    true,
				Priority:      PriorityParam{},
			},
			"\x00\x00\x03\x01\x05\x00\x00\x00*abc",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Flags:    FlagHeadersEndStream | FlagHeadersEndHeaders,
					Length:   uint32(len("abc")),
				},
				Priority:      PriorityParam{},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"with padding",
			HeadersFrameParam{
				StreamID:      42,
				BlockFragment: []byte("abc"),
				EndStream:     true,
				EndHeaders:    true,
				PadLength:     5,
				Priority:      PriorityParam{},
			},
			"\x00\x00\t\x01\r\x00\x00\x00*\x05abc\x00\x00\x00\x00\x00",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Flags:    FlagHeadersEndStream | FlagHeadersEndHeaders | FlagHeadersPadded,
					Length:   uint32(1 + len("abc") + 5), // pad length + contents + padding
				},
				Priority:      PriorityParam{},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"with priority",
			HeadersFrameParam{
				StreamID:      42,
				BlockFragment: []byte("abc"),
				EndStream:     true,
				EndHeaders:    true,
				PadLength:     2,
				Priority: PriorityParam{
					StreamDep: 15,
					Exclusive: true,
					Weight:    127,
				},
			},
			"\x00\x00\v\x01-\x00\x00\x00*\x02\x80\x00\x00\x0f\u007fabc\x00\x00",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Flags:    FlagHeadersEndStream | FlagHeadersEndHeaders | FlagHeadersPadded | FlagHeadersPriority,
					Length:   uint32(1 + 5 + len("abc") + 2), // pad length + priority + contents + padding
				},
				Priority: PriorityParam{
					StreamDep: 15,
					Exclusive: true,
					Weight:    127,
				},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"with priority stream dep zero", // golang.org/issue/15444
			HeadersFrameParam{
				StreamID:      42,
				BlockFragment: []byte("abc"),
				EndStream:     true,
				EndHeaders:    true,
				PadLength:     2,
				Priority: PriorityParam{
					StreamDep: 0,
					Exclusive: true,
					Weight:    127,
				},
			},
			"\x00\x00\v\x01-\x00\x00\x00*\x02\x80\x00\x00\x00\u007fabc\x00\x00",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Flags:    FlagHeadersEndStream | FlagHeadersEndHeaders | FlagHeadersPadded | FlagHeadersPriority,
					Length:   uint32(1 + 5 + len("abc") + 2), // pad length + priority + contents + padding
				},
				Priority: PriorityParam{
					StreamDep: 0,
					Exclusive: true,
					Weight:    127,
				},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"zero length",
			HeadersFrameParam{
				StreamID: 42,
				Priority: PriorityParam{},
			},
			"\x00\x00\x00\x01\x00\x00\x00\x00*",
			&HeadersFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 42,
					Type:     FrameHeaders,
					Length:   0,
				},
				Priority: PriorityParam{},
			},
		},
	}
	for _, tt := range tests {
		fr, buf := testFramer()
		if err := fr.WriteHeaders(tt.p); err != nil {
			t.Errorf("test %q: %v", tt.name, err)
			continue
		}
		if buf.String() != tt.wantEnc {
			t.Errorf("test %q: encoded %q; want %q", tt.name, buf.Bytes(), tt.wantEnc)
		}
		f, err := fr.ReadFrame()
		if err != nil {
			t.Errorf("test %q: failed to read the frame back: %v", tt.name, err)
			continue
		}
		if !reflect.DeepEqual(f, tt.wantFrame) {
			t.Errorf("test %q: mismatch.\n got: %#v\nwant: %#v\n", tt.name, f, tt.wantFrame)
		}
	}
}

func TestWriteInvalidStreamDep(t *testing.T) {
	fr, _ := testFramer()
	err := fr.WriteHeaders(HeadersFrameParam{
		StreamID: 42,
		Priority: PriorityParam{
			StreamDep: 1 << 31,
		},
	})
	if err != errDepStreamID {
		t.Errorf("header error = %v; want %q", err, errDepStreamID)
	}

	err = fr.WritePriority(2, PriorityParam{StreamDep: 1 << 31})
	if err != errDepStreamID {
		t.Errorf("priority error = %v; want %q", err, errDepStreamID)
	}
}

func TestWriteContinuation(t *testing.T) {
	const streamID = 42
	tests := []struct {
		name string
		end  bool
		frag []byte

		wantFrame *ContinuationFrame
	}{
		{
			"not end",
			false,
			[]byte("abc"),
			&ContinuationFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: streamID,
					Type:     FrameContinuation,
					Length:   uint32(len("abc")),
				},
				headerFragBuf: []byte("abc"),
			},
		},
		{
			"end",
			true,
			[]byte("def"),
			&ContinuationFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: streamID,
					Type:     FrameContinuation,
					Flags:    FlagContinuationEndHeaders,
					Length:   uint32(len("def")),
				},
				headerFragBuf: []byte("def"),
			},
		},
	}
	for _, tt := range tests {
		fr, _ := testFramer()
		if err := fr.WriteContinuation(streamID, tt.end, tt.frag); err != nil {
			t.Errorf("test %q: %v", tt.name, err)
			continue
		}
		fr.AllowIllegalReads = true
		f, err := fr.ReadFrame()
		if err != nil {
			t.Errorf("test %q: failed to read the frame back: %v", tt.name, err)
			continue
		}
		if !reflect.DeepEqual(f, tt.wantFrame) {
			t.Errorf("test %q: mismatch.\n got: %#v\nwant: %#v\n", tt.name, f, tt.wantFrame)
		}
	}
}

func TestParseRFC9218Priority(t *testing.T) {
	tests := []struct {
		name        string
		priorityStr string
		want        PriorityParam
		wantOk      bool
	}{
		{
			name:        "with urgency",
			priorityStr: "u=0",
			want: PriorityParam{
				urgency:     0,
				incremental: 0,
			},
			wantOk: true,
		},
		{
			name:        "with implicit incremental",
			priorityStr: "i",
			want: PriorityParam{
				urgency:     3,
				incremental: 1,
			},
			wantOk: true,
		},
		{
			name:        "with explicit incremental",
			priorityStr: "i=?1",
			want: PriorityParam{
				urgency:     3,
				incremental: 1,
			},
			wantOk: true,
		},
		{
			name:        "with urgency and incremental",
			priorityStr: "i=?0, u=4",
			want: PriorityParam{
				urgency:     4,
				incremental: 0,
			},
			wantOk: true,
		},
		{
			name:        "with other valid dictionary data",
			priorityStr: "some=data;someparam;u=fake, u=1;foo, i;bar",
			want: PriorityParam{
				urgency:     1,
				incremental: 1,
			},
			wantOk: true,
		},
		{
			name:        "repeated field",
			priorityStr: "u=1,i,u=5,i=?0",
			want: PriorityParam{
				urgency:     5,
				incremental: 0,
			},
			wantOk: true,
		},
		{
			name:        "wrong field type",
			priorityStr: `u="urgency will be ignored", i`,
			want: PriorityParam{
				urgency:     3,
				incremental: 1,
			},
			wantOk: true,
		},
		{
			name:        "invalid dictionary",
			priorityStr: `u=1,i, but this is not a valid dictionary"`,
			want:        defaultRFC9218Priority(true),
		},
		{
			name:        "out of range value",
			priorityStr: "u=8",
			want:        defaultRFC9218Priority(true),
			wantOk:      true,
		},
	}
	for _, tt := range tests {
		got, gotOk := parseRFC9218Priority(tt.priorityStr, true)
		if gotOk != tt.wantOk {
			t.Errorf("test %q: mismatch.\n got ok: %#v\nwant ok: %#v\n", tt.name, got, tt.want)
		}
		if got != tt.want {
			t.Errorf("test %q: mismatch.\n got: %#v\nwant: %#v\n", tt.name, got, tt.want)
		}
	}
}

func TestWritePriority(t *testing.T) {
	const streamID = 42
	tests := []struct {
		name      string
		priority  PriorityParam
		wantFrame *PriorityFrame
	}{
		{
			"not exclusive",
			PriorityParam{
				StreamDep: 2,
				Exclusive: false,
				Weight:    127,
			},
			&PriorityFrame{
				FrameHeader{
					valid:    true,
					StreamID: streamID,
					Type:     FramePriority,
					Length:   5,
				},
				PriorityParam{
					StreamDep: 2,
					Exclusive: false,
					Weight:    127,
				},
			},
		},

		{
			"exclusive",
			PriorityParam{
				StreamDep: 3,
				Exclusive: true,
				Weight:    77,
			},
			&PriorityFrame{
				FrameHeader{
					valid:    true,
					StreamID: streamID,
					Type:     FramePriority,
					Length:   5,
				},
				PriorityParam{
					StreamDep: 3,
					Exclusive: true,
					Weight:    77,
				},
			},
		},
	}
	for _, tt := range tests {
		fr, _ := testFramer()
		if err := fr.WritePriority(streamID, tt.priority); err != nil {
			t.Errorf("test %q: %v", tt.name, err)
			continue
		}
		f, err := fr.ReadFrame()
		if err != nil {
			t.Errorf("test %q: failed to read the frame back: %v", tt.name, err)
			continue
		}
		if !reflect.DeepEqual(f, tt.wantFrame) {
			t.Errorf("test %q: mismatch.\n got: %#v\nwant: %#v\n", tt.name, f, tt.wantFrame)
		}
	}
}

func TestWritePriorityUpdate(t *testing.T) {
	const streamID = 42
	tests := []struct {
		name      string
		priority  string
		wantFrame *PriorityUpdateFrame
	}{
		{
			name:     "with urgency",
			priority: "u=0",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   7,
				},
				Priority:            "u=0",
				PrioritizedStreamID: streamID,
			},
		},
		{
			name:     "with incremental",
			priority: "i",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   5,
				},
				Priority:            "i",
				PrioritizedStreamID: streamID,
			},
		},
		{
			name:     "with urgency and incremental",
			priority: "u=7,i",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   9,
				},
				Priority:            "u=7,i",
				PrioritizedStreamID: streamID,
			},
		},
		{
			name:     "with other fields",
			priority: "a=123,u=7,i,b;a;b",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   21,
				},
				Priority:            "a=123,u=7,i,b;a;b",
				PrioritizedStreamID: streamID,
			},
		},
		{
			name:     "with string escapes",
			priority: "u=\"invalid\" , i",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   19,
				},
				Priority:            "u=\"invalid\" , i",
				PrioritizedStreamID: streamID,
			},
		},
		{
			name:     "with empty payload",
			priority: "",
			wantFrame: &PriorityUpdateFrame{
				FrameHeader: FrameHeader{
					valid:    true,
					StreamID: 0,
					Type:     FramePriorityUpdate,
					Length:   4,
				},
				Priority:            "",
				PrioritizedStreamID: streamID,
			},
		},
	}
	for _, tt := range tests {
		fr, _ := testFramer()
		if err := fr.WritePriorityUpdate(streamID, tt.priority); err != nil {
			t.Errorf("test %q: %v", tt.name, err)
			continue
		}
		f, err := fr.ReadFrame()
		if err != nil {
			t.Errorf("test %q: failed to read the frame back: %v", tt.name, err)
			continue
		}
		if !reflect.DeepEqual(f, tt.wantFrame) {
			t.Errorf("test %q: mismatch.\n got: %#v\nwant: %#v\n", tt.name, f, tt.wantFrame)
		}
	}
}

func TestWriteSettings(t *testing.T) {
	fr, buf := testFramer()
	settings := []Setting{{1, 2}, {3, 4}}
	fr.WriteSettings(settings...)
	const wantEnc = "\x00\x00\f\x04\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x03\x00\x00\x00\x04"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	sf, ok := f.(*SettingsFrame)
	if !ok {
		t.Fatalf("Got a %T; want a SettingsFrame", f)
	}
	var got []Setting
	sf.ForeachSetting(func(s Setting) error {
		got = append(got, s)
		valBack, ok := sf.Value(s.ID)
		if !ok || valBack != s.Val {
			t.Errorf("Value(%d) = %v, %v; want %v, true", s.ID, valBack, ok, s.Val)
		}
		return nil
	})
	if !reflect.DeepEqual(settings, got) {
		t.Errorf("Read settings %+v != written settings %+v", got, settings)
	}
}

func TestWriteSettingsAck(t *testing.T) {
	fr, buf := testFramer()
	fr.WriteSettingsAck()
	const wantEnc = "\x00\x00\x00\x04\x01\x00\x00\x00\x00"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
}

func TestWriteWindowUpdate(t *testing.T) {
	fr, buf := testFramer()
	const streamID = 1<<24 + 2<<16 + 3<<8 + 4
	const incr = 7<<24 + 6<<16 + 5<<8 + 4
	if err := fr.WriteWindowUpdate(streamID, incr); err != nil {
		t.Fatal(err)
	}
	const wantEnc = "\x00\x00\x04\x08\x00\x01\x02\x03\x04\x07\x06\x05\x04"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	want := &WindowUpdateFrame{
		FrameHeader: FrameHeader{
			valid:    true,
			Type:     0x8,
			Flags:    0x0,
			Length:   0x4,
			StreamID: 0x1020304,
		},
		Increment: 0x7060504,
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("parsed back %#v; want %#v", f, want)
	}
}

// TestReadFrameReusesWindowUpdate verifies that ReadFrame returns the
// same *WindowUpdateFrame pointer for every WINDOW_UPDATE parsed, so
// that the parse path does not allocate a fresh struct each time. The
// reuse is unconditional (not gated by SetReuseFrames) because
// WindowUpdateFrame contains only value-type fields and all consumers
// in this package extract those fields synchronously.
func TestReadFrameReusesWindowUpdate(t *testing.T) {
	fr, buf := testFramer()

	// First read populates the cached struct.
	if err := fr.WriteWindowUpdate(1, 5); err != nil {
		t.Fatal(err)
	}
	first, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	firstWU, ok := first.(*WindowUpdateFrame)
	if !ok {
		t.Fatalf("first frame is %T, want *WindowUpdateFrame", first)
	}
	if firstWU.StreamID != 1 || firstWU.Increment != 5 {
		t.Fatalf("first WINDOW_UPDATE = %+v; want StreamID=1 Increment=5", firstWU)
	}

	// Subsequent WINDOW_UPDATEs return the same pointer with the new
	// values. Because the pointer is reused, the originally-returned
	// firstWU also reflects the latest fields after each ReadFrame.
	cases := []struct {
		streamID, increment uint32
	}{
		{streamID: 3, increment: 7},
		{streamID: 1, increment: 1024},
		{streamID: 1, increment: 1},
	}
	for i, tc := range cases {
		buf.Reset()
		if err := fr.WriteWindowUpdate(tc.streamID, tc.increment); err != nil {
			t.Fatal(err)
		}
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		wu, ok := f.(*WindowUpdateFrame)
		if !ok {
			t.Fatalf("iter %d: frame is %T, want *WindowUpdateFrame", i, f)
		}
		if wu != firstWU {
			t.Errorf("iter %d: pointer changed: have %p, want %p", i, wu, firstWU)
		}
		if wu.StreamID != tc.streamID || wu.Increment != tc.increment {
			t.Errorf("iter %d: got %+v; want StreamID=%d Increment=%d",
				i, wu, tc.streamID, tc.increment)
		}
		if firstWU.StreamID != tc.streamID || firstWU.Increment != tc.increment {
			t.Errorf("iter %d: firstWU = %+v; want StreamID=%d Increment=%d (reuse contract violated)",
				i, firstWU, tc.streamID, tc.increment)
		}
	}

	// Interleaving WINDOW_UPDATE with a different frame type does not
	// disturb reuse: the cached struct is re-validated when the next
	// WINDOW_UPDATE is parsed.
	buf.Reset()
	if err := fr.WritePing(false, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if err := fr.WriteWindowUpdate(5, 9000); err != nil {
		t.Fatal(err)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	wu, ok := f.(*WindowUpdateFrame)
	if !ok {
		t.Fatalf("after PING, frame is %T, want *WindowUpdateFrame", f)
	}
	if wu != firstWU {
		t.Errorf("after PING, pointer changed: have %p, want %p", wu, firstWU)
	}
	if wu.StreamID != 5 || wu.Increment != 9000 {
		t.Errorf("after PING, got %+v; want StreamID=5 Increment=9000", wu)
	}
}

// TestReadFrameWindowUpdateNoAllocs locks in the zero-allocation
// invariant for the WINDOW_UPDATE parse path. parseWindowUpdateFrame
// must not allocate; if it ever does, regressions show up here rather
// than only in benchmarks.
func TestReadFrameWindowUpdateNoAllocs(t *testing.T) {
	// Pre-encode a WINDOW_UPDATE frame.
	var enc bytes.Buffer
	if err := NewFramer(&enc, nil).WriteWindowUpdate(1, 7); err != nil {
		t.Fatal(err)
	}
	encoded := enc.Bytes()

	rbuf := bytes.NewReader(encoded)
	fr := NewFramer(io.Discard, rbuf)

	// Warm up the read buffer so its growth does not count toward the
	// measurement.
	rbuf.Reset(encoded)
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(50, func() {
		rbuf.Reset(encoded)
		if _, err := fr.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("ReadFrame for WINDOW_UPDATE allocates %v objects/op; want 0", allocs)
	}
}

// TestReadFrameWindowUpdateOverwrites is a defensive test against
// future maintenance hazards. The cached *WindowUpdateFrame is reused
// across ReadFrame calls, so any field that parseWindowUpdateFrame
// forgets to assign would leak its value from the previous frame to
// the next consumer.
//
// The test poisons every byte of the cached struct, parses a fresh
// WINDOW_UPDATE, and then verifies every field reflects the new frame
// rather than the poison. If a field is added to WindowUpdateFrame in
// the future, the corresponding assertion (and parseWindowUpdateFrame)
// must be updated.
func TestReadFrameWindowUpdateOverwrites(t *testing.T) {
	fr, buf := testFramer()

	// First read to obtain the cached struct pointer.
	if err := fr.WriteWindowUpdate(1, 5); err != nil {
		t.Fatal(err)
	}
	first, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	wuf, ok := first.(*WindowUpdateFrame)
	if !ok {
		t.Fatalf("first frame is %T, want *WindowUpdateFrame", first)
	}

	// Fill every byte of the cached struct with 0xFF. If any field is
	// left unassigned by the next parse, it will keep this poison value.
	poison := unsafe.Slice((*byte)(unsafe.Pointer(wuf)), unsafe.Sizeof(*wuf))
	for i := range poison {
		poison[i] = 0xFF
	}

	// Parse a fresh WINDOW_UPDATE. The poison must be fully gone.
	buf.Reset()
	const wantStreamID = 42
	const wantIncrement = 100
	if err := fr.WriteWindowUpdate(wantStreamID, wantIncrement); err != nil {
		t.Fatal(err)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	wu, ok := f.(*WindowUpdateFrame)
	if !ok {
		t.Fatalf("frame is %T, want *WindowUpdateFrame", f)
	}
	if wu != wuf {
		t.Fatalf("pointer changed after poison: have %p, want %p", wu, wuf)
	}

	// Check every field. Failure on any of these means
	// parseWindowUpdateFrame left a field unassigned, which would leak
	// data from a previous frame to the next consumer.
	if wu.Type != FrameWindowUpdate {
		t.Errorf("Type = %v (poison leak); want %v", wu.Type, FrameWindowUpdate)
	}
	if wu.Flags != 0 {
		t.Errorf("Flags = %#x (poison leak); want 0", wu.Flags)
	}
	if wu.Length != 4 {
		t.Errorf("Length = %d (poison leak); want 4", wu.Length)
	}
	if wu.StreamID != wantStreamID {
		t.Errorf("StreamID = %d (poison leak); want %d", wu.StreamID, wantStreamID)
	}
	if !wu.valid {
		t.Errorf("valid = false (poison leak); want true")
	}
	if wu.Increment != wantIncrement {
		t.Errorf("Increment = %d (poison leak); want %d", wu.Increment, wantIncrement)
	}
}

func TestWritePing(t *testing.T)    { testWritePing(t, false) }
func TestWritePingAck(t *testing.T) { testWritePing(t, true) }

func testWritePing(t *testing.T, ack bool) {
	fr, buf := testFramer()
	if err := fr.WritePing(ack, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	var wantFlags Flags
	if ack {
		wantFlags = FlagPingAck
	}
	var wantEnc = "\x00\x00\x08\x06" + string(wantFlags) + "\x00\x00\x00\x00" + "\x01\x02\x03\x04\x05\x06\x07\x08"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}

	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	want := &PingFrame{
		FrameHeader: FrameHeader{
			valid:    true,
			Type:     0x6,
			Flags:    wantFlags,
			Length:   0x8,
			StreamID: 0,
		},
		Data: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("parsed back %#v; want %#v", f, want)
	}
}

func TestReadFrameHeader(t *testing.T) {
	tests := []struct {
		in   string
		want FrameHeader
	}{
		{in: "\x00\x00\x00" + "\x00" + "\x00" + "\x00\x00\x00\x00", want: FrameHeader{}},
		{in: "\x01\x02\x03" + "\x04" + "\x05" + "\x06\x07\x08\x09", want: FrameHeader{
			Length: 66051, Type: 4, Flags: 5, StreamID: 101124105,
		}},
		// Ignore high bit:
		{in: "\xff\xff\xff" + "\xff" + "\xff" + "\xff\xff\xff\xff", want: FrameHeader{
			Length: 16777215, Type: 255, Flags: 255, StreamID: 2147483647}},
		{in: "\xff\xff\xff" + "\xff" + "\xff" + "\x7f\xff\xff\xff", want: FrameHeader{
			Length: 16777215, Type: 255, Flags: 255, StreamID: 2147483647}},
	}
	for i, tt := range tests {
		got, err := readFrameHeader(make([]byte, 9), strings.NewReader(tt.in))
		if err != nil {
			t.Errorf("%d. readFrameHeader(%q) = %v", i, tt.in, err)
			continue
		}
		tt.want.valid = true
		if !got.Equal(tt.want) {
			t.Errorf("%d. readFrameHeader(%q) = %+v; want %+v", i, tt.in, got, tt.want)
		}
	}
}

func TestReadWriteFrameHeader(t *testing.T) {
	tests := []struct {
		len      uint32
		typ      FrameType
		flags    Flags
		streamID uint32
	}{
		{len: 0, typ: 255, flags: 1, streamID: 0},
		{len: 0, typ: 255, flags: 1, streamID: 1},
		{len: 0, typ: 255, flags: 1, streamID: 255},
		{len: 0, typ: 255, flags: 1, streamID: 256},
		{len: 0, typ: 255, flags: 1, streamID: 65535},
		{len: 0, typ: 255, flags: 1, streamID: 65536},

		{len: 0, typ: 1, flags: 255, streamID: 1},
		{len: 255, typ: 1, flags: 255, streamID: 1},
		{len: 256, typ: 1, flags: 255, streamID: 1},
		{len: 65535, typ: 1, flags: 255, streamID: 1},
		{len: 65536, typ: 1, flags: 255, streamID: 1},
		{len: 16777215, typ: 1, flags: 255, streamID: 1},
	}
	for _, tt := range tests {
		fr, buf := testFramer()
		fr.startWrite(tt.typ, tt.flags, tt.streamID)
		fr.writeBytes(make([]byte, tt.len))
		fr.endWrite()
		fh, err := ReadFrameHeader(buf)
		if err != nil {
			t.Errorf("ReadFrameHeader(%+v) = %v", tt, err)
			continue
		}
		if fh.Type != tt.typ || fh.Flags != tt.flags || fh.Length != tt.len || fh.StreamID != tt.streamID {
			t.Errorf("ReadFrameHeader(%+v) = %+v; mismatch", tt, fh)
		}
	}

}

func TestWriteTooLargeFrame(t *testing.T) {
	fr, _ := testFramer()
	fr.startWrite(0, 1, 1)
	fr.writeBytes(make([]byte, 1<<24))
	err := fr.endWrite()
	if err != ErrFrameTooLarge {
		t.Errorf("endWrite = %v; want errFrameTooLarge", err)
	}
}

func TestWriteGoAway(t *testing.T) {
	const debug = "foo"
	fr, buf := testFramer()
	if err := fr.WriteGoAway(0x01020304, 0x05060708, []byte(debug)); err != nil {
		t.Fatal(err)
	}
	const wantEnc = "\x00\x00\v\a\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08" + debug
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	want := &GoAwayFrame{
		FrameHeader: FrameHeader{
			valid:    true,
			Type:     0x7,
			Flags:    0,
			Length:   uint32(4 + 4 + len(debug)),
			StreamID: 0,
		},
		LastStreamID: 0x01020304,
		ErrCode:      0x05060708,
		debugData:    []byte(debug),
	}
	if !reflect.DeepEqual(f, want) {
		t.Fatalf("parsed back:\n%#v\nwant:\n%#v", f, want)
	}
	if got := string(f.(*GoAwayFrame).DebugData()); got != debug {
		t.Errorf("debug data = %q; want %q", got, debug)
	}
}

func TestWritePushPromise(t *testing.T) {
	pp := PushPromiseParam{
		StreamID:      42,
		PromiseID:     42,
		BlockFragment: []byte("abc"),
	}
	fr, buf := testFramer()
	if err := fr.WritePushPromise(pp); err != nil {
		t.Fatal(err)
	}
	const wantEnc = "\x00\x00\x07\x05\x00\x00\x00\x00*\x00\x00\x00*abc"
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	_, ok := f.(*PushPromiseFrame)
	if !ok {
		t.Fatalf("got %T; want *PushPromiseFrame", f)
	}
	want := &PushPromiseFrame{
		FrameHeader: FrameHeader{
			valid:    true,
			Type:     0x5,
			Flags:    0x0,
			Length:   0x7,
			StreamID: 42,
		},
		PromiseID:     42,
		headerFragBuf: []byte("abc"),
	}
	if !reflect.DeepEqual(f, want) {
		t.Fatalf("parsed back:\n%#v\nwant:\n%#v", f, want)
	}
}

// test checkFrameOrder and that HEADERS and CONTINUATION frames can't be intermingled.
func TestReadFrameOrder(t *testing.T) {
	head := func(f *Framer, id uint32, end bool) {
		f.WriteHeaders(HeadersFrameParam{
			StreamID:      id,
			BlockFragment: []byte("foo"), // unused, but non-empty
			EndHeaders:    end,
		})
	}
	cont := func(f *Framer, id uint32, end bool) {
		f.WriteContinuation(id, end, []byte("foo"))
	}

	tests := [...]struct {
		name    string
		w       func(*Framer)
		atLeast int
		wantErr string
	}{
		0: {
			w: func(f *Framer) {
				head(f, 1, true)
			},
		},
		1: {
			w: func(f *Framer) {
				head(f, 1, true)
				head(f, 2, true)
			},
		},
		2: {
			wantErr: "got HEADERS for stream 2; expected CONTINUATION following HEADERS for stream 1",
			w: func(f *Framer) {
				head(f, 1, false)
				head(f, 2, true)
			},
		},
		3: {
			wantErr: "got DATA for stream 1; expected CONTINUATION following HEADERS for stream 1",
			w: func(f *Framer) {
				head(f, 1, false)
			},
		},
		4: {
			w: func(f *Framer) {
				head(f, 1, false)
				cont(f, 1, true)
				head(f, 2, true)
			},
		},
		5: {
			wantErr: "got CONTINUATION for stream 2; expected stream 1",
			w: func(f *Framer) {
				head(f, 1, false)
				cont(f, 2, true)
				head(f, 2, true)
			},
		},
		6: {
			wantErr: "unexpected CONTINUATION for stream 1",
			w: func(f *Framer) {
				cont(f, 1, true)
			},
		},
		7: {
			wantErr: "unexpected CONTINUATION for stream 1",
			w: func(f *Framer) {
				cont(f, 1, false)
			},
		},
		8: {
			wantErr: "HEADERS frame with stream ID 0",
			w: func(f *Framer) {
				head(f, 0, true)
			},
		},
		9: {
			wantErr: "unexpected CONTINUATION for stream 0",
			w: func(f *Framer) {
				cont(f, 0, true)
			},
		},
		10: {
			wantErr: "unexpected CONTINUATION for stream 1",
			atLeast: 5,
			w: func(f *Framer) {
				head(f, 1, false)
				cont(f, 1, false)
				cont(f, 1, false)
				cont(f, 1, false)
				cont(f, 1, true)
				cont(f, 1, false)
			},
		},
	}
	for i, tt := range tests {
		buf := new(bytes.Buffer)
		f := NewFramer(buf, buf)
		f.AllowIllegalWrites = true
		tt.w(f)
		f.WriteData(1, true, nil) // to test transition away from last step

		var err error
		n := 0
		var log bytes.Buffer
		for {
			var got Frame
			got, err = f.ReadFrame()
			fmt.Fprintf(&log, "  read %v, %v\n", got, err)
			if err != nil {
				break
			}
			n++
		}
		if err == io.EOF {
			err = nil
		}
		ok := tt.wantErr == ""
		if ok && err != nil {
			t.Errorf("%d. after %d good frames, ReadFrame = %v; want success\n%s", i, n, err, log.Bytes())
			continue
		}
		if !ok && err != ConnectionError(ErrCodeProtocol) {
			t.Errorf("%d. after %d good frames, ReadFrame = %v; want ConnectionError(ErrCodeProtocol)\n%s", i, n, err, log.Bytes())
			continue
		}
		if !((f.errDetail == nil && tt.wantErr == "") || (fmt.Sprint(f.errDetail) == tt.wantErr)) {
			t.Errorf("%d. framer error = %q; want %q\n%s", i, f.errDetail, tt.wantErr, log.Bytes())
		}
		if n < tt.atLeast {
			t.Errorf("%d. framer only read %d frames; want at least %d\n%s", i, n, tt.atLeast, log.Bytes())
		}
	}
}

func TestMetaFrameHeader(t *testing.T) {
	write := func(f *Framer, frags ...[]byte) {
		for i, frag := range frags {
			end := (i == len(frags)-1)
			if i == 0 {
				f.WriteHeaders(HeadersFrameParam{
					StreamID:      1,
					BlockFragment: frag,
					EndHeaders:    end,
				})
			} else {
				f.WriteContinuation(1, end, frag)
			}
		}
	}

	want := func(flags Flags, length uint32, pairs ...string) *MetaHeadersFrame {
		mh := &MetaHeadersFrame{
			HeadersFrame: &HeadersFrame{
				FrameHeader: FrameHeader{
					Type:     FrameHeaders,
					Flags:    flags,
					Length:   length,
					StreamID: 1,
				},
			},
			Fields: []hpack.HeaderField(nil),
		}
		for len(pairs) > 0 {
			mh.Fields = append(mh.Fields, hpack.HeaderField{
				Name:  pairs[0],
				Value: pairs[1],
			})
			pairs = pairs[2:]
		}
		return mh
	}
	truncated := func(mh *MetaHeadersFrame) *MetaHeadersFrame {
		mh.Truncated = true
		return mh
	}

	const noFlags Flags = 0

	oneKBString := strings.Repeat("a", 1<<10)

	tests := [...]struct {
		name              string
		w                 func(*Framer)
		want              any // *MetaHeaderFrame or error
		wantErrReason     string
		maxHeaderListSize uint32
	}{
		0: {
			name: "single_headers",
			w: func(f *Framer) {
				all := encodeHeaderRaw(t, ":method", "GET", ":path", "/")
				write(f, all)
			},
			want: want(FlagHeadersEndHeaders, 2, ":method", "GET", ":path", "/"),
		},
		1: {
			name: "with_continuation",
			w: func(f *Framer) {
				all := encodeHeaderRaw(t, ":method", "GET", ":path", "/", "foo", "bar")
				write(f, all[:1], all[1:])
			},
			want: want(noFlags, 1, ":method", "GET", ":path", "/", "foo", "bar"),
		},
		2: {
			name: "with_two_continuation",
			w: func(f *Framer) {
				all := encodeHeaderRaw(t, ":method", "GET", ":path", "/", "foo", "bar")
				write(f, all[:2], all[2:4], all[4:])
			},
			want: want(noFlags, 2, ":method", "GET", ":path", "/", "foo", "bar"),
		},
		3: {
			name: "big_string_okay",
			w: func(f *Framer) {
				all := encodeHeaderRaw(t, ":method", "GET", ":path", "/", "foo", oneKBString)
				write(f, all[:2], all[2:])
			},
			want: want(noFlags, 2, ":method", "GET", ":path", "/", "foo", oneKBString),
		},
		4: {
			name: "big_string_error",
			w: func(f *Framer) {
				all := encodeHeaderRaw(t, ":method", "GET", ":path", "/", "foo", oneKBString)
				write(f, all[:2], all[2:])
			},
			maxHeaderListSize: (1 << 10) / 2,
			want:              ConnectionError(ErrCodeCompression),
		},
		5: {
			name: "max_header_list_truncated",
			w: func(f *Framer) {
				var pairs = []string{":method", "GET", ":path", "/"}
				for range 100 {
					pairs = append(pairs, "foo", "bar")
				}
				all := encodeHeaderRaw(t, pairs...)
				write(f, all[:2], all[2:])
			},
			maxHeaderListSize: (1 << 10) / 2,
			want: truncated(want(noFlags, 2,
				":method", "GET",
				":path", "/",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar", // 11
			)),
		},
		6: {
			name: "pseudo_order",
			w: func(f *Framer) {
				write(f, encodeHeaderRaw(t,
					":method", "GET",
					"foo", "bar",
					":path", "/", // bogus
				))
			},
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: "pseudo header field after regular",
		},
		7: {
			name: "pseudo_unknown",
			w: func(f *Framer) {
				write(f, encodeHeaderRaw(t,
					":unknown", "foo", // bogus
					"foo", "bar",
				))
			},
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: "invalid pseudo-header \":unknown\"",
		},
		8: {
			name: "pseudo_mix_request_response",
			w: func(f *Framer) {
				write(f, encodeHeaderRaw(t,
					":method", "GET",
					":status", "100",
				))
			},
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: "mix of request and response pseudo headers",
		},
		9: {
			name: "pseudo_dup",
			w: func(f *Framer) {
				write(f, encodeHeaderRaw(t,
					":method", "GET",
					":method", "POST",
				))
			},
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: "duplicate pseudo-header \":method\"",
		},
		10: {
			name: "trailer_okay_no_pseudo",
			w:    func(f *Framer) { write(f, encodeHeaderRaw(t, "foo", "bar")) },
			want: want(FlagHeadersEndHeaders, 8, "foo", "bar"),
		},
		11: {
			name:          "invalid_field_name",
			w:             func(f *Framer) { write(f, encodeHeaderRaw(t, "CapitalBad", "x")) },
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: "invalid header field name \"CapitalBad\"",
		},
		12: {
			name:          "invalid_field_value",
			w:             func(f *Framer) { write(f, encodeHeaderRaw(t, "key", "bad_null\x00")) },
			want:          streamError(1, ErrCodeProtocol),
			wantErrReason: `invalid header field value for "key"`,
		},
	}
	for i, tt := range tests {
		buf := new(bytes.Buffer)
		f := NewFramer(buf, buf)
		f.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)
		f.MaxHeaderListSize = tt.maxHeaderListSize
		tt.w(f)

		name := tt.name
		if name == "" {
			name = fmt.Sprintf("test index %d", i)
		}

		var got any
		var err error
		got, err = f.ReadFrame()
		if err != nil {
			got = err

			// Ignore the StreamError.Cause field, if it matches the wantErrReason.
			// The test table above predates the Cause field.
			if se, ok := err.(StreamError); ok && se.Cause != nil && se.Cause.Error() == tt.wantErrReason {
				se.Cause = nil
				got = se
			}
		}
		if !reflect.DeepEqual(got, tt.want) {
			if mhg, ok := got.(*MetaHeadersFrame); ok {
				if mhw, ok := tt.want.(*MetaHeadersFrame); ok {
					hg := mhg.HeadersFrame
					hw := mhw.HeadersFrame
					if hg != nil && hw != nil && !reflect.DeepEqual(*hg, *hw) {
						t.Errorf("%s: headers differ:\n got: %+v\nwant: %+v\n", name, *hg, *hw)
					}
				}
			}
			str := func(v any) string {
				if _, ok := v.(error); ok {
					return fmt.Sprintf("error %v", v)
				} else {
					return fmt.Sprintf("value %#v", v)
				}
			}
			t.Errorf("%s:\n got: %v\nwant: %s", name, str(got), str(tt.want))
		}
		if tt.wantErrReason != "" && tt.wantErrReason != fmt.Sprint(f.errDetail) {
			t.Errorf("%s: got error reason %q; want %q", name, f.errDetail, tt.wantErrReason)
		}
	}
}

func TestSetReuseFrames(t *testing.T) {
	fr, buf := testFramer()
	fr.SetReuseFrames()

	// Check that DataFrames are reused. Note that
	// SetReuseFrames only currently implements reuse of DataFrames.
	firstDf := readAndVerifyDataFrame("ABC", 3, fr, buf, t)

	for range 10 {
		df := readAndVerifyDataFrame("XYZ", 3, fr, buf, t)
		if df != firstDf {
			t.Errorf("Expected Framer to return references to the same DataFrame. Have %v and %v", &df, &firstDf)
		}
	}

	for range 10 {
		df := readAndVerifyDataFrame("", 0, fr, buf, t)
		if df != firstDf {
			t.Errorf("Expected Framer to return references to the same DataFrame. Have %v and %v", &df, &firstDf)
		}
	}

	for range 10 {
		df := readAndVerifyDataFrame("HHH", 3, fr, buf, t)
		if df != firstDf {
			t.Errorf("Expected Framer to return references to the same DataFrame. Have %v and %v", &df, &firstDf)
		}
	}
}

func TestSetReuseFramesMoreThanOnce(t *testing.T) {
	fr, buf := testFramer()
	fr.SetReuseFrames()

	firstDf := readAndVerifyDataFrame("ABC", 3, fr, buf, t)
	fr.SetReuseFrames()

	for range 10 {
		df := readAndVerifyDataFrame("XYZ", 3, fr, buf, t)
		// SetReuseFrames should be idempotent
		fr.SetReuseFrames()
		if df != firstDf {
			t.Errorf("Expected Framer to return references to the same DataFrame. Have %v and %v", &df, &firstDf)
		}
	}
}

func TestNoSetReuseFrames(t *testing.T) {
	fr, buf := testFramer()
	const numNewDataFrames = 10
	dfSoFar := make([]any, numNewDataFrames)

	// Check that DataFrames are not reused if SetReuseFrames wasn't called.
	// SetReuseFrames only currently implements reuse of DataFrames.
	for i := range numNewDataFrames {
		df := readAndVerifyDataFrame("XYZ", 3, fr, buf, t)
		for _, item := range dfSoFar {
			if df == item {
				t.Errorf("Expected Framer to return new DataFrames since SetNoReuseFrames not set.")
			}
		}
		dfSoFar[i] = df
	}
}

func readAndVerifyDataFrame(data string, length byte, fr *Framer, buf *bytes.Buffer, t *testing.T) *DataFrame {
	var streamID uint32 = 1<<24 + 2<<16 + 3<<8 + 4
	fr.WriteData(streamID, true, []byte(data))
	wantEnc := "\x00\x00" + string(length) + "\x00\x01\x01\x02\x03\x04" + data
	if buf.String() != wantEnc {
		t.Errorf("encoded as %q; want %q", buf.Bytes(), wantEnc)
	}
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	df, ok := f.(*DataFrame)
	if !ok {
		t.Fatalf("got %T; want *DataFrame", f)
	}
	if !bytes.Equal(df.Data(), []byte(data)) {
		t.Errorf("got %q; want %q", df.Data(), []byte(data))
	}
	if f.Header().Flags&1 == 0 {
		t.Errorf("didn't see END_STREAM flag")
	}
	return df
}

// TestReadMetaFrameReusesFieldsSlice verifies that readMetaFrame
// reuses the backing array of MetaHeadersFrame.Fields across calls,
// avoiding a fresh allocation per HEADERS frame parsed. The
// MetaHeadersFrame documentation already requires callers not to
// retain the slice past the next ReadFrame, and the package's
// consumers ((*serverConn).processHeaders and
// (*clientConnReadLoop).processHeaders) copy field strings into
// their own header maps synchronously before returning, so reuse is
// safe.
func TestReadMetaFrameReusesFieldsSlice(t *testing.T) {
	fr, _ := testFramer()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	writeMeta := func(streamID uint32, fields ...string) {
		t.Helper()
		block := encodeHeaderRaw(t, fields...)
		if err := fr.WriteHeaders(HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: block,
			EndHeaders:    true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	readMeta := func() *MetaHeadersFrame {
		t.Helper()
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		mh, ok := f.(*MetaHeadersFrame)
		if !ok {
			t.Fatalf("got %T, want *MetaHeadersFrame", f)
		}
		return mh
	}

	// First read populates the cached slice.
	writeMeta(1, ":method", "GET", ":path", "/a", ":scheme", "http", ":authority", "x")
	mh1 := readMeta()
	if len(mh1.Fields) != 4 {
		t.Fatalf("first parse: got %d fields, want 4", len(mh1.Fields))
	}
	backing1 := unsafe.SliceData(mh1.Fields)

	// Second read of the same shape must reuse the backing array.
	writeMeta(3, ":method", "POST", ":path", "/b", ":scheme", "https", ":authority", "y")
	mh2 := readMeta()
	if len(mh2.Fields) != 4 {
		t.Fatalf("second parse: got %d fields, want 4", len(mh2.Fields))
	}
	if backing := unsafe.SliceData(mh2.Fields); backing != backing1 {
		t.Errorf("Fields backing array changed between reads: have %p, want %p", backing, backing1)
	}
	// The retained mh1 pointer now sees the second read's values,
	// because the slice is shared. Document that.
	if mh1.Fields[0].Name != mh2.Fields[0].Name {
		t.Errorf("retained mh1 view: mh1.Fields[0].Name = %q, mh2.Fields[0].Name = %q; expected equal after reuse",
			mh1.Fields[0].Name, mh2.Fields[0].Name)
	}

	// A larger read may grow the slice; capture the new backing array
	// and confirm a subsequent equal-size read reuses *that*.
	writeMeta(5,
		":method", "GET", ":path", "/c", ":scheme", "http", ":authority", "z",
		"x-a", "1", "x-b", "2", "x-c", "3", "x-d", "4")
	mh3 := readMeta()
	if len(mh3.Fields) != 8 {
		t.Fatalf("third parse: got %d fields, want 8", len(mh3.Fields))
	}
	backing3 := unsafe.SliceData(mh3.Fields)

	writeMeta(7,
		":method", "GET", ":path", "/d", ":scheme", "http", ":authority", "z",
		"x-a", "5", "x-b", "6", "x-c", "7", "x-d", "8")
	mh4 := readMeta()
	if backing := unsafe.SliceData(mh4.Fields); backing != backing3 {
		t.Errorf("Fields backing array changed after equal-size read: have %p, want %p", backing, backing3)
	}
}

// TestReadMetaFrameFieldsCacheAllocs verifies that the Fields slice
// no longer reallocates per parse. The remaining allocations belong
// to the MetaHeadersFrame struct, the SetEmitFunc closure, and the
// HeadersFrame struct (those are addressed by separate CLs); this
// test isolates the parse path so the Fields-slice change is visible.
func TestReadMetaFrameFieldsCacheAllocs(t *testing.T) {
	// Pre-encode many distinct HEADERS frames into a single buffer so
	// each ReadFrame call consumes a fresh one (the hpack encoder is
	// stateful, so we can't replay the same bytes).
	const numFrames = 30
	var wireBuf bytes.Buffer
	wfr := NewFramer(&wireBuf, nil)
	for i := 0; i < numFrames; i++ {
		block := encodeHeaderRaw(t,
			":method", "GET", ":path", "/", ":scheme", "http", ":authority", "x")
		if err := wfr.WriteHeaders(HeadersFrameParam{
			StreamID:      uint32(2*i + 1),
			BlockFragment: block,
			EndHeaders:    true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	encoded := wireBuf.Bytes()

	rbuf := bytes.NewReader(encoded)
	fr := NewFramer(io.Discard, rbuf)
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	// Warm up: first read allocates the Fields backing array.
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	// Measure the steady-state per-parse allocations. The Fields
	// slice is the cache target; remaining allocs come from sources
	// other CLs in this series address.
	const runs = numFrames - 1
	allocs := testing.AllocsPerRun(runs-1, func() {
		if _, err := fr.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	})
	// Expected steady-state allocs after both this CL and the
	// closure-to-method CL:
	//   1 *HeadersFrame struct (parseHeadersFrame)
	//   1 *MetaHeadersFrame struct
	// = 2 total. Without the closure-to-method CL the count would be
	// 6 (4 more from the SetEmitFunc closure value plus its three
	// hoisted-to-heap captured locals). Without the Fields-slice
	// cache it would be one higher still (a fresh backing array per
	// parse).
	t.Logf("readMetaFrame steady-state allocs/op = %v", allocs)
	if allocs > 2 {
		t.Errorf("readMetaFrame allocs/op = %v; want <= 2 after Fields cache + emit-method warmup (regression?)", allocs)
	}
}

// TestReadMetaFrameFieldsClearsSensitiveTail verifies that the slot
// at the end of the cached Fields backing array (beyond the new
// MetaHeadersFrame.Fields' len) does NOT retain HeaderField string
// references from the previous request. Without this clearing,
// secret header values (Cookie, Authorization, HPACK Sensitive=true)
// stay reachable for the lifetime of the connection any time a
// shorter HEADERS frame follows a longer one.
func TestReadMetaFrameFieldsClearsSensitiveTail(t *testing.T) {
	fr, _ := testFramer()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	writeMeta := func(streamID uint32, fields ...string) {
		t.Helper()
		block := encodeHeaderRaw(t, fields...)
		if err := fr.WriteHeaders(HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: block,
			EndHeaders:    true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// First HEADERS includes a "sensitive" value we want to be sure is
	// not retained after the next, shorter parse.
	const secret = "TOKEN_THAT_MUST_NOT_LEAK"
	writeMeta(1,
		":method", "GET", ":path", "/", ":scheme", "http", ":authority", "x",
		"authorization", secret,
		"cookie", "session="+secret)
	first, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	firstWU := first.(*MetaHeadersFrame)
	// Sanity: the secret is in the slice.
	foundSecret := false
	for _, hf := range firstWU.Fields {
		if hf.Value == secret || hf.Value == "session="+secret {
			foundSecret = true
		}
	}
	if !foundSecret {
		t.Fatalf("test setup: secret value not found in first parse")
	}
	backing := unsafe.SliceData(firstWU.Fields)
	cap1 := cap(firstWU.Fields)
	if cap1 < 6 {
		t.Fatalf("test setup: expected backing array cap >= 6, got %d", cap1)
	}

	// Second HEADERS with only 4 fields. The cache slot at indices
	// 4..cap1-1 must NOT still hold the secret strings.
	writeMeta(3, ":method", "POST", ":path", "/p", ":scheme", "http", ":authority", "y")
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	// Inspect every slot in the backing array via unsafe.Slice so we
	// see the tail beyond the new MetaHeadersFrame.Fields' len.
	tail := unsafe.Slice(backing, cap1)
	for i, hf := range tail {
		if strings.Contains(hf.Name, "authorization") ||
			strings.Contains(hf.Name, "cookie") ||
			strings.Contains(hf.Value, secret) {
			t.Errorf("backing slot %d still holds sensitive data: %+v", i, hf)
		}
	}
}

// TestReadMetaFrameFieldsRetentionCap verifies that a single large
// HEADERS frame does not permanently inflate per-Framer memory: the
// cached slice's capacity is dropped above maxRetainedFields, so
// subsequent reads start with a fresh, small backing array.
func TestReadMetaFrameFieldsRetentionCap(t *testing.T) {
	fr, _ := testFramer()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)
	// Allow large header lists for this test.
	fr.MaxHeaderListSize = 1 << 20

	// Send one HEADERS frame with many fields to grow the cache cap.
	bigFields := []string{":method", "GET", ":path", "/", ":scheme", "http", ":authority", "x"}
	for i := 0; i < 200; i++ {
		bigFields = append(bigFields, fmt.Sprintf("x-h-%d", i), "v")
	}
	block := encodeHeaderRaw(t, bigFields...)
	if err := fr.WriteHeaders(HeadersFrameParam{
		StreamID: 1, BlockFragment: block, EndHeaders: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	// The cap should have been dropped because it exceeded the
	// internal threshold; the next read starts from nil.
	if got, max := cap(fr.metaFields), 64; got > max {
		t.Errorf("after large parse, cap(fr.metaFields) = %d; want <= %d (retention cap)", got, max)
	}
}

// TestReadMetaFrameClearsMetaStateMH verifies that fr.metaState.mh is
// cleared once readMetaFrame returns, so the just-returned
// *MetaHeadersFrame is releasable by GC even if the connection is
// then idle for a long time before the next HEADERS frame arrives.
func TestReadMetaFrameClearsMetaStateMH(t *testing.T) {
	fr, _ := testFramer()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	block := encodeHeaderRaw(t,
		":method", "GET", ":path", "/", ":scheme", "http", ":authority", "x")
	if err := fr.WriteHeaders(HeadersFrameParam{
		StreamID: 1, BlockFragment: block, EndHeaders: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if fr.metaState.mh != nil {
		t.Errorf("fr.metaState.mh = %p; want nil after readMetaFrame returns", fr.metaState.mh)
	}
}

// TestEmitMetaFieldNilGuard verifies that calling emitMetaField with
// no in-flight readMetaFrame (fr.metaState.mh == nil) is a safe
// no-op rather than a panic or a write into stale state. This is the
// defense-in-depth behavior the closure-to-method conversion relies
// on for safety after the original "lose reference to
// MetaHeadersFrame" defer was removed.
func TestEmitMetaFieldNilGuard(t *testing.T) {
	fr, _ := testFramer()
	if fr.metaState.mh != nil {
		t.Fatalf("test setup: fresh Framer should have nil metaState.mh")
	}
	// Calling without setting up readMetaFrame state must not panic
	// or modify anything observable.
	fr.emitMetaField(hpack.HeaderField{Name: "x", Value: "y"})
	if fr.metaState.mh != nil {
		t.Errorf("emitMetaField mutated metaState.mh: %p", fr.metaState.mh)
	}
}

// TestReadMetaFrameClearsMetaStateInvalid verifies that the error
// captured by emitMetaField in fr.metaState.invalid (which can wrap
// an attacker-controlled header name) is dropped on return from
// readMetaFrame, not held across idle periods.
func TestReadMetaFrameClearsMetaStateInvalid(t *testing.T) {
	fr, _ := testFramer()
	fr.ReadMetaHeaders = hpack.NewDecoder(initialHeaderTableSize, nil)

	// Header name with uppercase chars triggers headerFieldNameError
	// inside emitMetaField, which assigns fr.metaState.invalid.
	block := encodeHeaderRaw(t,
		":method", "GET", ":path", "/", ":scheme", "http", ":authority", "x",
		"CapitalBad", "v")
	if err := fr.WriteHeaders(HeadersFrameParam{
		StreamID: 1, BlockFragment: block, EndHeaders: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.ReadFrame(); err == nil {
		t.Fatal("expected stream error for invalid header name")
	}
	if fr.metaState.invalid != nil {
		t.Errorf("fr.metaState.invalid = %v; want nil after readMetaFrame returns", fr.metaState.invalid)
	}
}

func encodeHeaderRaw(t testing.TB, headers ...string) []byte {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for len(headers) > 0 {
		k, v := headers[0], headers[1]
		err := enc.WriteField(hpack.HeaderField{Name: k, Value: v})
		if err != nil {
			t.Fatalf("HPACK encoding error for %q/%q: %v", k, v, err)
		}
		headers = headers[2:]
	}
	return buf.Bytes()
}

func TestSettingsDuplicates(t *testing.T) {
	tests := []struct {
		settings []Setting
		want     bool
	}{
		{nil, false},
		{[]Setting{{ID: 1}}, false},
		{[]Setting{{ID: 1}, {ID: 2}}, false},
		{[]Setting{{ID: 1}, {ID: 2}}, false},
		{[]Setting{{ID: 1}, {ID: 2}, {ID: 3}}, false},
		{[]Setting{{ID: 1}, {ID: 2}, {ID: 3}}, false},
		{[]Setting{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, false},

		{[]Setting{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 2}}, true},
		{[]Setting{{ID: 4}, {ID: 2}, {ID: 3}, {ID: 4}}, true},

		{[]Setting{
			{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
			{ID: 5}, {ID: 6}, {ID: 7}, {ID: 8},
			{ID: 9}, {ID: 10}, {ID: 11}, {ID: 12},
		}, false},

		{[]Setting{
			{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
			{ID: 5}, {ID: 6}, {ID: 7}, {ID: 8},
			{ID: 9}, {ID: 10}, {ID: 11}, {ID: 11},
		}, true},
	}
	for i, tt := range tests {
		fr, _ := testFramer()
		fr.WriteSettings(tt.settings...)
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("%d. ReadFrame: %v", i, err)
		}
		sf := f.(*SettingsFrame)
		got := sf.HasDuplicates()
		if got != tt.want {
			t.Errorf("%d. HasDuplicates = %v; want %v", i, got, tt.want)
		}
	}

}

func TestTypeFrameParser(t *testing.T) {
	if len(frameNames) != len(frameParsers) {
		t.Errorf("expected len(frameNames)=%d to equal len(frameParsers)=%d",
			len(frameNames), len(frameParsers))
	}

	// typeFrameParser() for an unknown type returns a function that returns UnknownFrame
	unknownFrameType := FrameType(FramePriorityUpdate + 1)
	unknownParser := typeFrameParser(unknownFrameType)
	frame, err := unknownParser(nil, FrameHeader{}, nil, nil)
	if err != nil {
		t.Errorf("unknownParser() must not return an error: %v", err)
	}
	if _, isUnknown := frame.(*UnknownFrame); !isUnknown {
		t.Errorf("expected UnknownFrame, got %T", frame)
	}
}

func TestReadFrameHeaderAndBody(t *testing.T) {
	fr, _ := testFramer()
	var streamID uint32 = 1
	data := []byte("ABC")
	if err := fr.WriteData(streamID, true, data); err != nil {
		t.Fatalf("WriteData(%d, true, %q) failed: %v", streamID, data, err)
	}

	fh, err := fr.ReadFrameHeader()
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	wantHeader := FrameHeader{
		Type:     FrameData,
		Flags:    FlagDataEndStream,
		Length:   3,
		StreamID: 1,
		valid:    true,
	}
	if !fh.Equal(wantHeader) {
		t.Fatalf("ReadFrameHeader = %+v; want %+v", fh, wantHeader)
	}

	f, err := fr.ReadFrameForHeader(fh)
	if err != nil {
		t.Fatalf("ReadFrameForHeader failed: %v", err)
	}

	if !fh.Equal(f.Header()) {
		t.Fatalf("Frame.Header() = %+v; want %+v", f.Header(), fh)
	}

	df, ok := f.(*DataFrame)
	if !ok {
		t.Fatalf("got %T; want *DataFrame", f)
	}
	if got, want := df.Data(), data; !bytes.Equal(got, want) {
		t.Errorf("DataFrame.Data() = %q; want %q", string(got), string(want))
	}
	if got, want := df.StreamEnded(), true; got != want {
		t.Errorf("DataFrame.StreamEnded() = %v; want %v", got, want)
	}
}

func TestReadFrameHeaderFrameTooLarge(t *testing.T) {
	fr, _ := testFramer()
	fr.SetMaxReadFrameSize(2)
	if err := fr.WriteData(1, true, []byte("ABC")); err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}
	fh, err := fr.ReadFrameHeader()
	if gotErr, wantErr := err, ErrFrameTooLarge; gotErr != wantErr {
		t.Fatalf("ReadFrameHeader returned error %v; want %v", gotErr, wantErr)
	}
	if fh.StreamID != 1 {
		t.Errorf("ReadFrameHeader = %v, %v; want StreamID 1", fh, err)
	}
}

func TestReadFrameHeaderBadFrameOrder(t *testing.T) {
	fr, _ := testFramer()
	if err := fr.WriteHeaders(HeadersFrameParam{
		StreamID:      1,
		BlockFragment: []byte("foo"), // unused, but non-empty
		EndHeaders:    false,
	}); err != nil {
		t.Fatalf("WriteHeaders failed: %v", err)
	}

	// Write a CONTINUATION frame for stream 2 without first finishing the headers for stream 1.
	if err := fr.WriteContinuation(2, true, []byte("foo")); err != nil {
		t.Fatalf("WriteContinuation failed: %v", err)
	}

	fh, err := fr.ReadFrameHeader()
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if _, err = fr.ReadFrameForHeader(fh); err != nil {
		t.Fatalf("ReadFrameForHeader failed: %v", err)
	}

	if _, err := fr.ReadFrameHeader(); err != ConnectionError(ErrCodeProtocol) {
		t.Fatalf("ReadFrameHeader returned error %v; want ConnectionError(ErrCodeProtocol)", err)
	}
}

func TestReadFrameForHeaderUnexpectedEOF(t *testing.T) {
	fr, b := testFramer()
	if err := fr.WriteData(1, true, []byte("ABC")); err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	fh, err := fr.ReadFrameHeader()
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}

	// Remove one byte from the body, corrupting the frame body.
	b.Truncate(b.Len() - 1)

	_, err = fr.ReadFrameForHeader(fh)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("ReadFrameForHeader with short body = %v; want io.ErrUnexpectedEOF", err)
	}
}

func TestTypeFrameParserHolePanic(t *testing.T) {
	// Verify that unassigned frame types (0x0a-0x0f) don't panic. golang.org/issue/77652
	fr, _ := testFramer()
	if err := fr.WriteRawFrame(FrameType(0x0a), 0, 1, nil); err != nil {
		t.Fatal(err)
	}

	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := f.(*UnknownFrame); !ok {
		t.Errorf("got %T; want *UnknownFrame", f)
	}
}
