// Copyright 2023 github.com/pschou
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wormdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"sync"
)

type WormDB struct {
	fh        *os.File
	size      int // size of file
	BlockSize int // block size (for building index)

	Index       [][]byte
	IndexPrefix []uint8
	records     int

	Tree [256]searchTree

	// Buffers for creating a file
	write_buf [][]byte
	fh_buf    *bytes.Buffer
	index_buf *bytes.Buffer
	last      []byte
	readPool  sync.Pool
}

func (w *WormDB) SaveIndex(fh *os.File) {
	enc := gob.NewEncoder(fh)
	enc.Encode(w)
}

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

type searchTree struct {
	Index []uint8
	Tree  []searchTree
	Start uint32
}

func (st *searchTree) make(val uint8) *searchTree {
	if len(st.Tree) < 128 {
		for i, j := range st.Index {
			if j == val {
				return &st.Tree[i]
			}
		}
		st.Index = append(st.Index, val)
		st.Tree = append(st.Tree, searchTree{})
		return &st.Tree[len(st.Tree)-1]
	}
	if len(st.Tree) < 256 {
		tree := make([]searchTree, 256)
		for _, i := range st.Index {
			tree[i] = st.Tree[i]
		}
		st.Tree = tree
		st.Index = nil
	}
	return &st.Tree[val]
}
func (st *searchTree) get(val uint8) *searchTree {
	if len(st.Tree) < 256 {
		bi, bj := 0, uint8(0)
		for i, j := range st.Index {
			if j == val {
				return &st.Tree[i]
			}
			if bj < val && j > bj {
				bi, bj = i, j
			}
		}
		return &st.Tree[bi]
	}
	return &st.Tree[val]
}

type block struct {
	fh     *os.File
	offset int
	prefix uint8
	Start  []byte
}

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
