package codec

import (
	"bytes"
	"testing"

	gatewaypb "dify_gateway/api/proto"
)

func TestClientEnvelopeRoundTrip(t *testing.T) {
	in := &gatewaypb.ClientEnvelope{
		Body: &gatewaypb.ClientEnvelope_Chat{
			Chat: &gatewaypb.ChatRequest{
				RequestId:      "req-1",
				ConversationId: "conv-1",
				NpcId:          "npc-blacksmith",
				Query:          "你这里有什么好装备？",
				Context:        map[string]string{"scene": "town"},
			},
		},
	}

	var buf bytes.Buffer
	if err := WriteEnvelope(&buf, in); err != nil {
		t.Fatalf("WriteEnvelope() error = %v", err)
	}

	out, err := ReadClientEnvelope(&buf)
	if err != nil {
		t.Fatalf("ReadClientEnvelope() error = %v", err)
	}
	chat := out.GetChat()
	if chat == nil {
		t.Fatalf("body = %#v, want chat", out.GetBody())
	}
	if chat.GetRequestId() != "req-1" || chat.GetNpcId() != "npc-blacksmith" {
		t.Fatalf("chat = %#v", chat)
	}
	if chat.GetQuery() != "你这里有什么好装备？" {
		t.Fatalf("query = %q", chat.GetQuery())
	}
	if chat.GetContext()["scene"] != "town" {
		t.Fatalf("context = %#v", chat.GetContext())
	}
}

func TestServerEnvelopePingRoundTrip(t *testing.T) {
	// Pong marshals to an empty body inside a non-empty oneof; exercises the
	// zero-length-ish path through the framing.
	in := &gatewaypb.ServerEnvelope{
		Body: &gatewaypb.ServerEnvelope_Done{
			Done: &gatewaypb.ChatDone{RequestId: "req-9", ConversationId: "conv-9", TotalTokens: 42},
		},
	}

	var buf bytes.Buffer
	if err := WriteEnvelope(&buf, in); err != nil {
		t.Fatalf("WriteEnvelope() error = %v", err)
	}
	out, err := ReadServerEnvelope(&buf)
	if err != nil {
		t.Fatalf("ReadServerEnvelope() error = %v", err)
	}
	done := out.GetDone()
	if done == nil || done.GetRequestId() != "req-9" || done.GetTotalTokens() != 42 {
		t.Fatalf("done = %#v", done)
	}
}

func TestEmptyClientEnvelopeRoundTrip(t *testing.T) {
	// An envelope with no oneof set marshals to zero bytes — must still frame
	// and unframe cleanly (validates zero-length frame handling end to end).
	var buf bytes.Buffer
	if err := WriteEnvelope(&buf, &gatewaypb.ClientEnvelope{}); err != nil {
		t.Fatalf("WriteEnvelope() error = %v", err)
	}
	out, err := ReadClientEnvelope(&buf)
	if err != nil {
		t.Fatalf("ReadClientEnvelope() error = %v", err)
	}
	if out.GetBody() != nil {
		t.Fatalf("body = %#v, want nil", out.GetBody())
	}
}
