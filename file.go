package wormdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Create a new worm-db using a ReaderAtWriter as storage.
// For example, one can use an *os.File.
func New(fh ReaderAtWriter, bloomSize int) (*DB, error) {
	ret := &DB{fh: fh, blockSize: 4096, header: header}
	_, err := fh.WriteAt([]byte("WORMDB00"), 0)
	if err != nil {
		return nil, err
	}
	// Create a new empty header
	hdr := &header{B: bloomSize, T: time.Now()}
	var d bytes.Buffer
	enc := gob.NewEncoder(&d)
	err = enc.Encode(hdr)
	if err != nil {
		return nil, err
	}

	ret.readPool = sync.Pool{
		New: func() any {
			b := make([]byte, ret.blockSize)
			return &b
		},
	}
	return ret, nil
}

// Write the headers, bloom filter, and index to disk
func (d DB) Sync() error {
}

type saveDB struct {
	BlockSize   int // block size (for building index)
	Tree        [256]searchTree
	IndexPrefix []uint8
	Index       [][]byte
}

// Save the index into a writer
func (w *DB) SaveIndex(fh io.Writer) error {
	_, err := fh.Write([]byte("WORMIX"))
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(fh)
	return enc.Encode(saveDB{
		BlockSize:   w.blockSize,
		Index:       w.index,
		IndexPrefix: w.indexPrefix,
		Tree:        w.tree,
	})
}

// Save the index into a file
func (w *DB) SaveIndexFile(file string) error {
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
func Load(db ReaderAtWriter, idx io.Reader) (*DB, error) {
	buf := make([]byte, 6)
	n, err := db.ReadAt(buf, 0)
	if n != 6 || string(buf) != "WORMDB" {
		return nil, errors.New("Invalid WORMDB data header")
	}

	n, err = idx.Read(buf)
	if n != 6 || string(buf) != "WORMIX" {
		return nil, errors.New("Invalid WORMDB index header")
	}

	dec := gob.NewDecoder(idx)
	load := new(saveDB)
	err = dec.Decode(load)
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
