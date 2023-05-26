package wormdb

import (
	"bytes"
	"errors"
)

// Search for an entry in the database and return the full entry found or error.
func (w *DB) Find(qry []byte) ([]byte, error) {
	base := &w.tree[qry[0]]
	pos := base.Start

	for i := 1; i < len(qry); i++ {
		if len(base.Tree) == 0 {
			break
		}
		base = base.get(qry[i])
		if base == nil {
			break
		}
		pos = base.Start
	}

	prefix := w.indexPrefix[pos-1]
	if int(prefix) > len(qry) {
		return nil, errors.New("Query too short for exact matching")
	}

	first := w.index[pos-1]
	if len(first) < len(qry) {
		return nil, errors.New("Query is longer than the data")
	}

	if cmp := bytes.Compare(first[:len(qry)], qry); cmp == 0 {
		// Easy win as the value matched the index
		return first, nil
	} else if cmp > 0 {
		// The index value is already larger than what is requested
		return nil, nil
	}

	// Advance if needed
	for int(pos) < len(w.index) {
		next := w.index[pos]
		if len(next) < len(qry) {
			return nil, errors.New("Query is longer than the data")
		}
		if cmp := bytes.Compare(next[:len(qry)], qry); cmp == 0 {
			// Easy win as the value matched the index
			return first, nil
		} else if cmp < 0 {
			// Next is still less, step forward
			prefix = w.indexPrefix[pos]
			pos++
			first = next
		} else if cmp > 0 {
			// Next would be too far
			break
		}
	}

	if prefix > 0 {
		if cmp := bytes.Compare(first[:prefix], qry[:prefix]); cmp != 0 {
			// No match as the value is out of range of this block
			return nil, nil
		}
	}

	bufp := w.readPool.Get().(*[]byte)
	defer w.readPool.Put(bufp)

	// Read the block for finding the entry
	_, err := w.fh.ReadAt(*bufp, int64(w.blockSize)*int64(pos-1))
	if err != nil {
		return nil, err
	}

	b := *bufp
	minSz := len(qry) - int(prefix)
	// Loop over block looking for the record
	for sz := b[0]; sz > 0 && len(b) > int(sz); sz = b[0] {
		if int(sz) >= minSz {
			if cmp := bytes.Compare(b[1:minSz+1], qry[prefix:]); cmp == 0 {
				// Value matched
				ret := make([]byte, int(prefix)+int(sz))
				copy(ret, first[:prefix])
				copy(ret[prefix:], b[1:])
				return ret, nil
			} else if cmp > 0 {
				// The next value is already larger than what is requested
				return nil, nil
			}
		}
		b = b[sz+1:]
	}
	return nil, nil
}
