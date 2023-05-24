package wormdb

import (
	"bytes"
	"errors"
)

func (w *WormDB) Find(qry []byte) ([]byte, error) {
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
		return nil, errors.New("Query too short for exact matching")
	}

	first := w.Index[pos-1]
	if cmp := bytes.Compare(first[:prefix], qry[:prefix]); cmp != 0 {
		// No match as the value is out of range of this block
		return nil, nil
	}

	if len(first) > len(qry) {
		if cmp := bytes.Compare(first[prefix:len(qry)], qry[prefix:]); cmp == 0 {
			// Easy win as the value matched the index
			return first, nil
		} else if cmp > 0 {
			// The index value is already larger than what is requested
			return nil, nil
		}
	}

	// Advance if needed
	for int(pos) < len(w.Index) {
		next := w.Index[pos]
		if cmp := bytes.Compare(next[:len(qry)], qry); cmp == 0 {
			// Easy win as the value matched the index
			return first, nil
		} else if cmp < 0 {
			// Next is still less, step forward
			pos++
			first = next
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
		return nil, err
	}

	b := *bufp
	minSz := len(qry) - int(prefix)
	// Loop over block looking for the record
	for sz := b[0] + 1; sz > 0 && len(b) > int(sz); sz = b[0] + 1 {
		if int(sz) >= minSz {
			if cmp := bytes.Compare(b[1:minSz+1], qry[prefix:]); cmp == 0 {
				// Easy win as the value matched the index
				return append(first[:prefix], b[1:sz]...), nil
			} else if cmp > 0 {
				// The first value is already larger than what is requested
				return nil, nil
			}
		}
		b = b[sz:]
	}
	return nil, nil //&block{fh: w.fh, offset: w.BlockSize * int(pos-1), prefix: w.IndexPrefix[pos-1], Start: w.Index[pos-1]}
}
