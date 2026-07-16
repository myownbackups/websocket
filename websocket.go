package websocket

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"net/http"
)

type MessageType byte
type Option struct {
	DisCompression bool
}

func SetClientHeadersWithOption(headers http.Header, option *Option) {
	p := make([]byte, 16)
	io.ReadFull(rand.Reader, p)
	headers.Set("Upgrade", "websocket")
	headers.Set("Connection", "Upgrade")
	headers.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(p))
	headers.Set("Sec-WebSocket-Version", "13")
	if option != nil {
		if !option.DisCompression {
			headers.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits")
		}
	}
}

const (
	ContinuationMessage MessageType = 0x0
	TextMessage         MessageType = 0x1
	BinaryMessage       MessageType = 0x2
	CloseMessage        MessageType = 0x8
	PingMessage         MessageType = 0x9
	PongMessage         MessageType = 0xa
)
