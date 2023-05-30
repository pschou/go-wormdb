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
	"bufio"
	"bytes"
	"io"
	"sync"
)

type DB struct {
	fh        ReaderAtWriter
	size      int // size of file
	blockSize int // block size (for building index)

	index       [][]byte
	indexPrefix []uint8
	records     int

	tree [256]searchTree

	// Buffers for creating a file
	write_buf [][]byte
	fh_buf    *bufio.Writer
	index_buf *bytes.Buffer
	last      []byte
	readPool  sync.Pool
}

type ReaderAtWriter interface {
	io.ReaderAt
	io.WriterAt
	io.Writer
}
