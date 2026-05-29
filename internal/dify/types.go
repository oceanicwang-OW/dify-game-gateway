package dify

import "io"

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

type UploadFileReq struct {
	User     string
	Filename string
	Reader   io.Reader
	// ContentType is the MIME type for the file part. If empty it is inferred
	// from the filename extension (see fileContentType).
	ContentType string
}

type UploadFileResult struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type Parameters struct {
	UserInputForm    []ParameterFormItem
	FileUpload       FileUploadConfig
	OpeningStatement string
}

type ParameterFormItem struct {
	Type     string
	Label    string
	Variable string
	Required bool
	Default  string
}

type FileUploadConfig struct {
	Image struct {
		Enabled      bool `json:"enabled"`
		NumberLimits int  `json:"number_limits"`
	} `json:"image"`
}
