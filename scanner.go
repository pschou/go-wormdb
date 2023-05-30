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
)

type Scanner struct {
	fh          ReaderAtWriter
	db          *DB
	indexPos    int
	index       []byte
	indexPrefix uint8
	buf, cur    []byte
}

func (d *DB) NewScanner() *Scanner {
	return &Scanner{fh: d.fh, db: d, buf: make([]byte, d.blockSize)}
}

// Scan to the next record in the database.  One must use Scan() to get the initial line.
func (w *Scanner) Scan() bool {
	if w.fh != w.db.fh {
		// The file has been reopened
		return false
	}
	for {
		// Keep reading until we find data
		if len(w.cur) == 0 || w.cur[0] == 0 {
			// Buffer is empty, fill it
			if w.indexPos >= len(w.db.index) {
				return false
			}

			// Load next indexPos
			n, err := w.fh.ReadAt(w.buf, int64(w.indexPos*w.db.blockSize))
			if err != nil && err != io.EOF {
				return false
			}
			if w.indexPos == 0 && n > 6 {
				w.cur = w.buf[6:]
			} else {
				w.cur = w.buf
			}
			w.index = w.db.index[w.indexPos]
			w.indexPrefix = w.db.indexPrefix[w.indexPos]
			w.indexPos++
		} else {
			// If there is already data, advance to next record
			sz := w.cur[0]
			if int(sz)+1 > len(w.cur) {
				// Bad state, should not get here
				return false
			}
			w.cur = w.cur[sz+1:]
		}

		if len(w.cur) > 0 {
			sz := w.cur[0]
			if sz > 0 && int(sz)+1 <= len(w.cur) {
				return true
			}
		}
	}
}

// Return the current record in []byte format
func (w *Scanner) Bytes() []byte {
	if len(w.cur) == 0 || int(w.cur[0])+1 > len(w.cur) {
		return nil
	}
	return append(w.index[:w.indexPrefix], w.cur[1:w.cur[0]+1]...)
}

// Return the current record in string format
func (w *Scanner) Text() string {
	return string(w.Bytes())
}
