package wormdb

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"sync"
)

// Create a new worm-db using a file as storage.
func New(fh *os.File) *DB {
	ret := &DB{fh: fh, blockSize: 4096, fh_buf: new(bytes.Buffer), index_buf: new(bytes.Buffer)}
	ret.readPool = sync.Pool{
		New: func() any {
			b := make([]byte, ret.blockSize)
			return &b
		},
	}
	return ret
}

type saveDB struct {
	BlockSize   int // block size (for building index)
	Tree        [256]searchTree
	IndexPrefix []uint8
	Index       [][]byte
}

// Save the index into a writer
func (w *DB) SaveIndex(fh io.Writer) error {
	enc := gob.NewEncoder(fh)
	return enc.Encode(saveDB{
		BlockSize:   w.blockSize,
		Index:       w.index,
		IndexPrefix: w.indexPrefix,
		Tree:        w.tree,
	})
}

// Save the index into a file
func (w *DB) SaveIndexFiles(file string) error {
	// Save off the index for future reloading
	idx, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer idx.Close()
	return w.SaveIndex(idx)
}

// Load a worm-db and index for usage.
func LoadFiles(db, idx string) (*DB, error) {
	dbf, err := os.Open(db)
	if err != nil {
		return nil, err
	}
	idxf, err := os.Open(idx)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()
	return Load(dbf, idxf)
}

// Load a worm-db and index for usage.
func Load(db, idx *os.File) (*DB, error) {
	dec := gob.NewDecoder(idx)
	load := new(saveDB)
	err := dec.Decode(load)
	if err != nil {
		return nil, err
	}
	return &DB{
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
