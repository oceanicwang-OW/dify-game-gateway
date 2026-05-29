package codec

import (
	"fmt"
	"io"

	gatewaypb "dify_gateway/api/proto"

	"google.golang.org/protobuf/proto"
)

// WriteEnvelope marshals a gateway envelope (ClientEnvelope or ServerEnvelope,
// or any proto.Message) and writes it as a single length-prefixed frame.
func WriteEnvelope(w io.Writer, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("codec: marshal envelope: %w", err)
	}
	return WriteFrame(w, payload)
}

// ReadClientEnvelope reads one frame and unmarshals it as a ClientEnvelope
// (gateway-inbound: auth/chat/stop/ping).
func ReadClientEnvelope(r io.Reader) (*gatewaypb.ClientEnvelope, error) {
	payload, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	env := &gatewaypb.ClientEnvelope{}
	if err := proto.Unmarshal(payload, env); err != nil {
		return nil, fmt.Errorf("codec: unmarshal client envelope: %w", err)
	}
	return env, nil
}

// ReadServerEnvelope reads one frame and unmarshals it as a ServerEnvelope
// (gateway-outbound: auth_result/chunk/done/error/pong/blocked). Useful for
// tests and a future client implementation.
func ReadServerEnvelope(r io.Reader) (*gatewaypb.ServerEnvelope, error) {
	payload, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	env := &gatewaypb.ServerEnvelope{}
	if err := proto.Unmarshal(payload, env); err != nil {
		return nil, fmt.Errorf("codec: unmarshal server envelope: %w", err)
	}
	return env, nil
}
