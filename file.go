package wormdb

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

// Create a new worm-db using a file as storage.
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

// Save the index into memory
func (w *WormDB) SaveIndex(fh *os.File) {
	enc := gob.NewEncoder(fh)
	enc.Encode(w)
}

// Load a worm-db and index for usage.
func Load(db, idx *os.File) (*WormDB, error) {
	dec := gob.NewDecoder(idx)
	ret := new(WormDB)
	err := dec.Decode(ret)
	if err != nil {
		return nil, err
	}
	ret.fh = db
	ret.readPool = sync.Pool{
		New: func() any {
			b := make([]byte, ret.BlockSize)
			return &b
		},
	}
	return ret, nil
}
