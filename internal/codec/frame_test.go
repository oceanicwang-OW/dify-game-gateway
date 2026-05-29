package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestWriteReadFrameRoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"empty":  {},
		"small":  []byte("hello gateway"),
		"binary": {0x00, 0x01, 0xff, 0x7f, 0x80},
		"max":    bytes.Repeat([]byte{0xab}, MaxFrameSize),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, payload); err != nil {
				t.Fatalf("WriteFrame() error = %v", err)
			}
			if buf.Len() != lengthPrefixSize+len(payload) {
				t.Fatalf("framed len = %d, want %d", buf.Len(), lengthPrefixSize+len(payload))
			}

			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame() error = %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}

func TestReadFrameMultipleFramesInSequence(t *testing.T) {
	var buf bytes.Buffer
	payloads := [][]byte{[]byte("first"), {}, []byte("third frame")}
	for _, p := range payloads {
		if err := WriteFrame(&buf, p); err != nil {
			t.Fatalf("WriteFrame() error = %v", err)
		}
	}
	for i, want := range payloads {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame(#%d) error = %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame #%d = %q, want %q", i, got, want)
		}
	}
}

func TestWriteFrameRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	oversized := make([]byte, MaxFrameSize+1)
	err := WriteFrame(&buf, oversized)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("WriteFrame() error = %v, want ErrFrameTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("WriteFrame() wrote %d bytes on rejection, want 0", buf.Len())
	}
}

func TestReadFrameRejectsOversizedHeaderWithoutReadingBody(t *testing.T) {
	var header [lengthPrefixSize]byte
	binary.BigEndian.PutUint32(header[:], MaxFrameSize+1)

	// Only the header is present; no body bytes follow. A correct implementation
	// rejects on the declared length without attempting to read the body.
	r := bytes.NewReader(header[:])
	_, err := ReadFrame(r)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadFrame() error = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameCleanEOFOnNoData(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadFrame() error = %v, want io.EOF", err)
	}
}

func TestReadFrameTruncatedHeader(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader([]byte{0x00, 0x01}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadFrame() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadFrameTruncatedBody(t *testing.T) {
	var header [lengthPrefixSize]byte
	binary.BigEndian.PutUint32(header[:], 10) // claims 10 bytes
	data := append(header[:], []byte("only4")...)
	_, err := ReadFrame(bytes.NewReader(data))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadFrame() error = %v, want io.ErrUnexpectedEOF", err)
	}
}
