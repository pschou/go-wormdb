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
