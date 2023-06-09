package wormdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

func calculateSize(dat [][]byte, stripPrefix int) (sz int) {
	for _, d := range dat {
		sz += len(d) - stripPrefix + 1
	}
	return
}
func prefixLen(a, b []byte) int {
	for i := range a {
		if i == len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

// Add entries to the database.  They must already be in byte order!
// Note: Add is not thread safe.
func (w *DB) Add(d []byte) (err error) {
	if w.fh_buf == nil {
		return errors.New("Cannot add record, already finalized")
	}
	if len(w.write_buf) == 0 {
		// Add entry to the write buffer
		w.write_buf = [][]byte{append(d, []byte{}...)}
		return nil
	}
	last := w.write_buf[len(w.write_buf)-1]
	if bytes.Compare(last, d) != -1 {
		return fmt.Errorf("Out of order data %q > %q", last, d)
	}

	intraBlock := w.size % w.blockSize
	prefix := prefixLen(w.write_buf[0], d)
	next := calculateSize(append(w.write_buf, d), prefix)

	// If this new record would cause data to spill into a new block, then write
	// the current buffer and add an entry to our lookup tree
	if intraBlock+next > w.blockSize {
		err = w.writeBuf(true)
	}
	w.write_buf = append(w.write_buf, append(d, []byte{}...))
	return
}

func (w *DB) writeBuf(pad bool) (err error) {
	// Recalculate the prefix
	first := w.write_buf[0]
	last := w.write_buf[len(w.write_buf)-1]
	prefix := prefixLen(first, last)

	// Write to the index builder
	w.index_buf.WriteByte(byte(len(first)))
	w.index_buf.WriteByte(byte(prefix))
	w.index_buf.Write(first)

	// Walk the search tree
	tree := &w.tree[first[0]]

	pos := uint32(w.size/w.blockSize) + 1
	for i := 1; i < prefix+1 && i < len(first); i++ {
		if len(tree.Tree) == 0 {
			tree.Start = pos
		}
		tree = tree.make(first[i])
	}

	// Write the raw data to disk in the format: length (byte) and then data
	var n int
	for _, wd := range w.write_buf {
		wd = wd[prefix:]
		w.fh_buf.WriteByte(byte(len(wd)))
		n, err = w.fh_buf.Write(wd)
		if err != nil {
			return
		}
		w.size += n + 1
	}
	if pad {
		for w.size%w.blockSize > 0 {
			w.fh_buf.WriteByte(0)
			w.size++
		}
	} else {
		w.size++
		w.fh_buf.WriteByte(0)
	}
	w.write_buf = nil
	return
}

// Finalize the addition process, and write the index to disk (optional).
func (w *DB) Finalize() (err error) {
	if len(w.write_buf) > 0 {
		err = w.writeBuf(false)
		if err != nil {
			return
		}
	}
	w.fh_buf.Flush()
	if f, ok := w.fh.(*os.File); ok {
		f.Sync()
	}
	// Prevent reading more into memory
	w.fh_buf = nil

	// Make the index
	w.index = make([][]byte, (w.size+w.blockSize-1)/w.blockSize)
	w.indexPrefix = make([]uint8, (w.size+w.blockSize-1)/w.blockSize)
	for i := range w.index {
		size, _ := w.index_buf.ReadByte()
		w.indexPrefix[i], _ = w.index_buf.ReadByte()
		w.index[i] = make([]byte, size)
		w.index_buf.Read(w.index[i])
	}
	fillTree(1, w.tree[:])
	return
}

func fillTree(val uint32, base []searchTree) uint32 {
	for i, tree := range base {
		if tree.Start > 0 {
			val = tree.Start
		} else {
			base[i].Start = val
		}
		if len(tree.Tree) > 0 {
			val = fillTree(val, tree.Tree)
		}
	}
	return val
}
