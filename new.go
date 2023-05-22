package wormdb

import (
	"bytes"
	"os"
	"sync"
)

// Create a new worm-db
func New(fh *os.File) *WormDB {
	ret := &WormDB{fh: fh, BlockSize: 4096, fh_buf: new(bytes.Buffer), index_buf: new(bytes.Buffer)}
	ret.readPool = sync.Pool{
		New: func() any {
			b := make([]byte, ret.BlockSize)
			return &b
		},
	}
	return ret
}
