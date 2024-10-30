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
	"io"
	"time"
)

type DB struct {
	fh    ReaderWriterAt
	Bloom bloom.Filter

	Index     [][]byte
	IndexStep [256]int
	Updated   time.Time // Last updated
	BlockSize int       // block size for disk storage
}

var header struct {
	N uint64 // Entry count
	B uint64 // Bloom filter size in bytes
	S uint64 // Search tree size in bytes
	F uint64 // Fragment location (zero if not fragmented)
	H uint32 // Quickie hash for consistency
}

type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt
}
