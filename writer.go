package websocket

import (
	"bytes"
	"compress/flate"
	"io"
)

type writer struct {
	w    *flate.Writer
	dict *bytes.Buffer
	l    int
}

func (obj *writer) updateDict(p []byte) {
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
func (obj *writer) Write(p []byte) (n int, err error) {
	n, err = obj.w.Write(p)
	if n > 0 {
		obj.updateDict(p[:n])
	}
	return n, err
}
func (obj *writer) Flush() error {
	return obj.w.Flush()
}
func (obj *writer) Reset(w io.Writer) {
	obj.w.Close()
	fw, _ := flate.NewWriterDict(w, flate.BestCompression, obj.dict.Bytes())
	obj.w = fw
}
