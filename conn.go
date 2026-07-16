package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/gospider007/gson"
	"github.com/gospider007/re"
	"github.com/gospider007/tools"
)

type Conn struct {
	conn                io.ReadWriteCloser
	bit                 int
	isClient            bool
	maxLength           int
	helper              wsflate.Helper
	compressorContext   *writer
	decompressorContext *reader
	readLock            sync.Mutex
	writeLock           sync.Mutex
}

func (obj *Conn) ReadMessage() (MessageType, []byte, error) {
	obj.readLock.Lock()
	defer obj.readLock.Unlock()
	var lastFrame *ws.Frame
	for {
		frame, err := ws.ReadFrame(obj.conn)
		if err != nil {
			return 0, nil, err
		}
		if frame.Header.Masked {
			frame = ws.UnmaskFrame(frame)
		}
		if frame.Header.Fin && lastFrame == nil {
			if ok, err := wsflate.IsCompressed(frame.Header); ok && err == nil {
				if frame, err = obj.helper.DecompressFrame(frame); err != nil {
					return 0, nil, err
				}
			}
			return MessageType(frame.Header.OpCode), frame.Payload, nil
		}
		switch frame.Header.OpCode {
		case ws.OpContinuation:
			if lastFrame == nil {
				return 0, nil, errors.New("invalid message")
			}
			lastFrame.Header.Fin = frame.Header.Fin
			lastFrame.Payload = append(lastFrame.Payload, frame.Payload...)
		case ws.OpText, ws.OpBinary:
			if lastFrame != nil {
				return 0, nil, errors.New("invalid message")
			}
			lastFrame = &frame
		default:
			return 0, nil, errors.New("invalid message")
		}
		if lastFrame.Header.Fin {
			if ok, err := wsflate.IsCompressed(lastFrame.Header); ok && err == nil {
				if *lastFrame, err = obj.helper.DecompressFrame(*lastFrame); err != nil {
					return 0, nil, err
				}
			}
			return MessageType(lastFrame.Header.OpCode), lastFrame.Payload, nil
		}
	}
}

func (obj *Conn) writeMeta(messageType MessageType, fin bool, data []byte) (err error) {
	frame := ws.NewFrame(ws.OpCode(messageType), fin, data)
	if obj.isClient {
		frame = ws.MaskFrame(frame)
	}
	return ws.WriteFrame(obj.conn, frame)
}

func (obj *Conn) WriteMessage(messageType MessageType, value any) error {
	obj.writeLock.Lock()
	defer obj.writeLock.Unlock()
	var p []byte
	var err error
	switch vv := value.(type) {
	case []byte:
		p = vv
	case string:
		p = tools.StringToBytes(vv)
	default:
		p, err = gson.Encode(value)
		if err != nil {
			return err
		}
	}
	frame := ws.NewFrame(ws.OpCode(messageType), true, p)
	if obj.helper.Compressor != nil {
		if frame, err = obj.helper.CompressFrame(frame); err != nil {
			return err
		}
	}
	if obj.maxLength <= 0 || frame.Header.Length <= int64(obj.maxLength) {
		if obj.isClient {
			frame = ws.MaskFrame(frame)
		}
		return ws.WriteFrame(obj.conn, frame)
	} else {
		p = frame.Payload[obj.maxLength:]
		err = obj.writeMeta(messageType, false, frame.Payload[:obj.maxLength])
		if err != nil {
			return err
		}
	}
	for {
		if len(p) <= obj.maxLength {
			return obj.writeMeta(ContinuationMessage, true, p)
		} else {
			err = obj.writeMeta(ContinuationMessage, false, p[:obj.maxLength])
			if err != nil {
				return err
			}
			p = p[obj.maxLength:]
		}
	}
}
func (obj *Conn) Close() error {
	return obj.conn.Close()
}
func NewConn(conn io.ReadWriteCloser, isClient bool, Extension string) *Conn {
	con := Conn{conn: conn, isClient: isClient}
	con.helper.Decompressor = con.decompressor
	if strings.Contains(Extension, "permessage-deflate") {
		con.helper.Compressor = con.compressor
	}
	if isClient && strings.Contains(Extension, "client_no_context_takeover") {
		con.helper.Compressor = func(w io.Writer) wsflate.Compressor {
			f, _ := flate.NewWriter(w, flate.BestCompression)
			return f
		}
		con.helper.Decompressor = func(r io.Reader) wsflate.Decompressor {
			f := flate.NewReader(r)
			return f
		}
	}
	if !isClient && strings.Contains(Extension, "server_no_context_takeover") {
		con.helper.Compressor = func(w io.Writer) wsflate.Compressor {
			f, _ := flate.NewWriter(w, flate.BestCompression)
			return f
		}
		con.helper.Decompressor = func(r io.Reader) wsflate.Decompressor {
			f := flate.NewReader(r)
			return f
		}
	}
	if bitRs := re.Search(`client_max_window_bits=(\d+)`, Extension); bitRs != nil {
		con.bit, _ = strconv.Atoi(bitRs.Group(1))
	} else if bitRs := re.Search(`server_max_window_bits=(\d+)`, Extension); bitRs != nil {
		con.bit, _ = strconv.Atoi(bitRs.Group(1))
	}
	return &con
}
func (obj *Conn) newReader(r io.Reader) *reader {
	bit := 15
	if obj.bit > 0 {
		bit = obj.bit
	}
	return &reader{
		l:    1 << uint(bit),
		dict: bytes.NewBuffer(nil),
		r:    flate.NewReader(r),
	}
}
func (obj *Conn) newWriter(w io.Writer) *writer {
	bit := 15
	if obj.bit > 0 {
		bit = obj.bit
	}
	fw, _ := flate.NewWriterDict(w, flate.BestCompression, nil)
	return &writer{
		l:    1 << uint(bit),
		dict: bytes.NewBuffer(nil),
		w:    fw,
	}
}

func (obj *Conn) IsClient() bool {
	return obj.isClient
}
