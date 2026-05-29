package dify

// ChatReq is the gateway's request contract for Dify chat messages.
type ChatReq struct {
	Query            string
	Inputs           map[string]string
	User             string
	ConversationID   string
	Files            []FileRef
	AutoGenerateName *bool
}

type FileRef struct {
	Type           string `json:"type"`
	TransferMethod string `json:"transfer_method"`
	UploadFileID   string `json:"upload_file_id"`
}

type ChatResult struct {
	Answer         string
	ConversationID string
	MessageID      string
	TotalTokens    int
}
