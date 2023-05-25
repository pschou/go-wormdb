package wormdb

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"sync"
)

// Create a new worm-db using a file as storage.
func New(fh *os.File) *WormDB {
	ret := &WormDB{fh: fh, blockSize: 4096, fh_buf: new(bytes.Buffer), index_buf: new(bytes.Buffer)}
	ret.readPool = sync.Pool{
		New: func() any {
			b := make([]byte, ret.blockSize)
			return &b
		},
	}
	return ret
}

type saveWormDB struct {
	BlockSize   int // block size (for building index)
	Tree        [256]searchTree
	IndexPrefix []uint8
	Index       [][]byte
}

// Save the index into a file
func (w *WormDB) SaveIndex(fh io.Writer) {
	enc := gob.NewEncoder(fh)
	enc.Encode(saveWormDB{
		BlockSize:   w.blockSize,
		Index:       w.index,
		IndexPrefix: w.indexPrefix,
		Tree:        w.tree,
	})
}

// Load a worm-db and index for usage.
func Load(db, idx *os.File) (*WormDB, error) {
	dec := gob.NewDecoder(idx)
	load := new(saveWormDB)
	err := dec.Decode(load)
	if err != nil {
		return nil, err
	}
	return &WormDB{
		blockSize:   load.BlockSize,
		index:       load.Index,
		indexPrefix: load.IndexPrefix,
		tree:        load.Tree,
		fh:          db,
		readPool: sync.Pool{
			New: func() any {
				b := make([]byte, load.BlockSize)
				return &b
			},
		},
	}, nil
}
