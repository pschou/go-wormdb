package wormdb

import (
	"bytes"
	"errors"
)

// Update an entry in the database, note that the entry cannot move in relation to the other values
func (w *WormDB) Update(qry, updated []byte) error {
	base := &w.Tree[qry[0]]
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

	prefix := w.IndexPrefix[pos-1]
	if int(prefix) > len(qry) {
		return errors.New("Query too short for exact matching")
	}

	first := w.Index[pos-1]
	if cmp := bytes.Compare(first[:prefix], qry[:prefix]); cmp != 0 {
		// No match as the value is out of range of this block
		return errors.New("No match, before prefixed value")
	}

	if len(first) >= len(qry) {
		if cmp := bytes.Compare(first[prefix:len(qry)], qry[prefix:]); cmp == 0 {
			// Easy win as the value matched the index
			w.Index[pos-1] = updated
			return nil
		} else if cmp > 0 {
			// The index value is already larger than what is requested
			return errors.New("No match, before indexed value")
		}
	}

	// Advance if needed
	for int(pos) < len(w.Index) {
		next := w.Index[pos]
		if cmp := bytes.Compare(next[:len(qry)], qry); cmp == 0 {
			// Easy win as the value matched the index
			w.Index[pos] = updated
			return nil
		} else if cmp < 0 {
			// Next is still less, step forward
			pos++
			first = next
			prefix = w.IndexPrefix[pos]
		} else if cmp > 0 {
			// Next would be too far
			break
		}
	}

	bufp := w.readPool.Get().(*[]byte)
	defer w.readPool.Put(bufp)

	// Read the block for finding the entry
	_, err := w.fh.ReadAt(*bufp, int64(w.BlockSize)*int64(pos-1))
	if err != nil {
		return err
	}

	b := *bufp
	i := 0
	minSz := len(qry) - int(prefix)
	// Loop over block looking for the record
	for sz := b[0] + 1; sz > 0 && len(b) > int(sz); sz = b[0] + 1 {
		if int(sz) >= minSz {
			if cmp := bytes.Compare(b[1:minSz+1], qry[prefix:]); cmp == 0 {
				// Value matched
				if int(prefix)+int(sz)-1 != len(updated) {
					return errors.New("Length for current value and update must match")
				}
				copy(b[1:], updated[prefix:])
				_, err := w.fh.WriteAt(*bufp, int64(w.BlockSize)*int64(pos-1))
				return err
			} else if cmp > 0 {
				// The next value is already larger than what is requested
				return errors.New("No match, before indexed value")
			}
		}
		b = b[sz:]
		i += int(sz)
	}
	return errors.New("No match, end of search")
}
