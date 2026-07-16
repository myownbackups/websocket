package websocket

import (
	"io"

	"github.com/gobwas/ws/wsflate"
)

func (obj *Conn) decompressor(r io.Reader) wsflate.Decompressor {
	if obj.decompressorContext == nil {
		obj.decompressorContext = obj.newReader(r)
	} else {
		obj.decompressorContext.Reset(r)
	}
	return obj.decompressorContext
}
func (obj *Conn) compressor(w io.Writer) wsflate.Compressor {
	if obj.compressorContext == nil {
		obj.compressorContext = obj.newWriter(w)
	} else {
		obj.compressorContext.Reset(w)
	}
	return obj.compressorContext
}
