// Package codec implements the client<->gateway wire format: a 4-byte
// big-endian length prefix followed by a Protobuf payload (PDR §4.1), plus
// helpers to (un)marshal the gateway envelopes onto that framing.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize is the upper bound on a single frame's payload (PDR §4.1).
// Frames declaring or carrying more must be rejected and the connection closed.
const MaxFrameSize = 4 << 20 // 4 MB

// lengthPrefixSize is the size of the big-endian length prefix in bytes.
const lengthPrefixSize = 4

// ErrFrameTooLarge is returned when a frame's payload exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("codec: frame exceeds max size")

// WriteFrame writes payload as a single length-prefixed frame. A zero-length
// payload is valid (e.g. a Ping that marshals to no bytes). It returns
// ErrFrameTooLarge without writing anything if the payload is too large.
func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(payload))
	}

	buf := make([]byte, lengthPrefixSize+len(payload))
	binary.BigEndian.PutUint32(buf[:lengthPrefixSize], uint32(len(payload)))
	copy(buf[lengthPrefixSize:], payload)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("codec: write frame: %w", err)
	}
	return nil
}

// ReadFrame reads exactly one length-prefixed frame. The declared length is
// validated against MaxFrameSize before any payload is allocated, so an
// oversized header is rejected without reading the body. A short read of the
// header surfaces io.EOF (clean close) or io.ErrUnexpectedEOF (truncated).
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [lengthPrefixSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	n := binary.BigEndian.Uint32(header[:])
	if n > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	if n == 0 {
		return []byte{}, nil
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}
