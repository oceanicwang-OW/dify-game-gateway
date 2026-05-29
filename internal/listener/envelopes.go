package listener

import gatewaypb "dify_gateway/api/proto"

// Small constructors for the ServerEnvelope variants the access layer emits
// directly. Chat-path envelopes (chunk/done/blocked) are built by the Handler.

func pong() *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{Body: &gatewaypb.ServerEnvelope_Pong{Pong: &gatewaypb.Pong{}}}
}

func authResult(ok bool, reason string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{
		Body: &gatewaypb.ServerEnvelope_AuthResult{
			AuthResult: &gatewaypb.AuthResult{Ok: ok, Reason: reason},
		},
	}
}

func errorMsg(requestID, code, message string) *gatewaypb.ServerEnvelope {
	return &gatewaypb.ServerEnvelope{
		Body: &gatewaypb.ServerEnvelope_Error{
			Error: &gatewaypb.ErrorMsg{RequestId: requestID, Code: code, Message: message},
		},
	}
}
