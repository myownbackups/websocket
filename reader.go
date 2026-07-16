package websocket

import (
	"bytes"
	"compress/flate"
	"io"
)

type reader struct {
	r    io.ReadCloser
	dict *bytes.Buffer
	l    int
}

func (obj *reader) updateDict(p []byte) {
	if len(p) == 0 {
		return
	}
	pL := len(p)
	if pL >= obj.l {
		obj.dict.Reset()
		p = p[pL-obj.l:]
	} else if yL := obj.dict.Len() + pL; yL > obj.l {
		obj.dict.Next(yL - obj.l)
	}
	obj.dict.Write(p)
}
func (obj *reader) Read(p []byte) (n int, err error) {
	n, err = obj.r.Read(p)
	if n > 0 {
		obj.updateDict(p[:n])
	}
	return n, err
}
func (obj *reader) Reset(r io.Reader) {
	obj.r.(flate.Resetter).Reset(r, obj.dict.Bytes())
}
