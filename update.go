package wormdb

import (
	"bytes"
	"errors"
)

// Update an entry in the database, note that the entry cannot move in relation
// to the other values nor change size.
func (w *DB) Update(qry, updated []byte) error {
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
		return errors.New("Query too short for exact matching")
	}

	first := w.index[pos-1]
	if len(first) >= len(qry) {
		if cmp := bytes.Compare(first[:len(qry)], qry); cmp == 0 {
			// Easy win as the value matched the index
			w.index[pos-1] = updated
			return nil
		} else if cmp > 0 {
			// The index value is already larger than what is requested
			return errors.New("No match, before indexed value")
		}
	}

	// Advance if needed
	for int(pos) < len(w.index) {
		next := w.index[pos]
		if cmp := bytes.Compare(next[:len(qry)], qry); cmp == 0 {
			// Easy win as the value matched the index
			w.index[pos] = updated
			return nil
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
			return errors.New("No match, before prefixed value")
		}
	}

	bufp := w.readPool.Get().(*[]byte)
	defer w.readPool.Put(bufp)

	// Read the block for finding the entry
	_, err := w.fh.ReadAt(*bufp, int64(w.blockSize)*int64(pos-1))
	if err != nil {
		return err
	}

	b := *bufp
	i := 0
	minSz := len(qry) - int(prefix)
	// Loop over block looking for the record
	for sz := b[0]; sz > 0 && len(b) > int(sz); sz = b[0] {
		if int(sz) >= minSz {
			if cmp := bytes.Compare(b[1:minSz+1], qry[prefix:]); cmp == 0 {
				// Value matched
				if int(prefix)+int(sz) != len(updated) {
					return errors.New("Length for current value and update must match")
				}
				copy(b[1:], updated[prefix:])
				_, err := w.fh.WriteAt(*bufp, int64(w.blockSize)*int64(pos-1))
				return err
			} else if cmp > 0 {
				// The next value is already larger than what is requested
				return errors.New("No match, before indexed value")
			}
		}
		b = b[int(sz)+1:]
		i += int(sz) + 1
	}
	return errors.New("No match, end of search")
}
