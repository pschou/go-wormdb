package wormdb

import (
	"bytes"
	"container/list"
	"fmt"
	"slices"
)

type Search interface {
	// When building an index, this is called upon each insertion and can be used
	// to build a more complex searching method.
	//
	// If one plans on doing complex sorting logic, it is recommended to use a
	// pre-forked-goroutine and a sized input channel to absorb the needles as
	// they are loaded so as to free up the reading method (and maintain
	// insertion order) so as to continue to write to disk additional records
	// until the next block is surpassed.  Also-- after the wormdb has been fully
	// loaded, one must make sure that this input channel has been flushed out
	// before querying the wormdb.
	Add(needle []byte) error

	// Look up a record and determine the sector on disk to read from.
	Find(needle []byte) (sectorId int, lower []byte, wasExactMatch bool)

	// Return the lower and upper bounds of a block for the given needle.
	FindBounds(needle []byte) (sectorId int, lower, upper []byte, wasExactMatch bool)

	// Called after last record has been added.
	Finalize()
}

type BinarySearch struct {
	Index                [][]byte
	list                 *list.List
	lowerByte, upperByte []int
}

// Add a record into the searchable list.  Usually this involves an in-memory
// cache of the first record in each block and built using a link list so as to
// avoid growing memory and doing a slice copy.
func (s *BinarySearch) Add(needle []byte) error {
	if s.Index != nil {
		return fmt.Errorf("Could not add %q as search has been finalized", needle)
	}
	if s.list == nil {
		s.list = list.New()
	}
	s.list.PushBack(needle)
	return nil
}

// Do not call this directly, but instead wormdb calls this once the database
// has been flushed to disk.
func (s *BinarySearch) Finalize() {
	if s.list != nil {
		var list *list.List
		list, s.list = s.list, nil
		s.Index = make([][]byte, list.Len())
		for i, e := 0, list.Front(); e != nil; i, e = i+1, e.Next() {
			s.Index[i] = e.Value.([]byte)
		}
		s.MakeFirstByte()
	}
}

// After a database has been loaded into memory from a save, call this to build
// the lower and upper byte bounds for faster searching capabilities.
func (s *BinarySearch) MakeFirstByte() {
	return
	var (
		lb    = make([]int, 256)
		ub    = make([]int, 256)
		cur   = byte(255)
		upper = len(s.Index)
	)
	for i := upper - 1; i >= 0; i-- {
		if s.Index[i][0] != cur {
			for cur > s.Index[i][0] {
				ub[cur] = upper
				lb[cur] = i
				cur--
			}
			upper = i + 1
		}
	}
	for cur > 0 {
		ub[cur] = upper
		cur--
	}
	ub[0] = upper
	s.lowerByte, s.upperByte = lb, ub
	//log.Printf("lb: %#v\n", s.lowerByte)
	//log.Printf("ub: %#v\n", s.upperByte)
}

// Find will search for a needle in the Index and return either the match or
// the lower bound where the match would be located between two entries.  The
// purpose of the lower bound is to ensure that the match will be contained in
// the block retrieved from slow storage, such as a disk.
func (s *BinarySearch) Find(needle []byte) (pos int, lower []byte, exactMatch bool) {
	if len(s.lowerByte) > 0 {
		fb := needle[0]
		pos, exactMatch = slices.BinarySearchFunc(s.Index[s.lowerByte[fb]:s.upperByte[fb]], needle, bytes.Compare)
		pos += s.lowerByte[fb]
	} else {
		pos, exactMatch = slices.BinarySearchFunc(s.Index, needle, bytes.Compare)
	}
	if !exactMatch {
		if pos == 0 {
			// Try providing the first
			if bytes.HasPrefix(s.Index[0], needle) {
				return 0, s.Index[0], true
			}
			// If the record is before the first, give up
			return 0, nil, false
		}
		// Go back one step
		pos--
	}
	return pos, s.Index[pos], exactMatch
}

// FindBounds will search for a needle in the Index and return either the match
// or the lower bound where the match would be located between two entries.
// The purpose of the lower bound is to ensure that the match will be contained
// in the block retrieved from slow storage (such as a disk) and the upper
// bound is useful for segmenting data to make sure the result lies within the
// block.
func (s *BinarySearch) FindBounds(needle []byte) (pos int, lower, upper []byte, exactMatch bool) {
	if len(s.lowerByte) > 0 {
		fb := needle[0]
		pos, exactMatch = slices.BinarySearchFunc(s.Index[s.lowerByte[fb]:s.upperByte[fb]], needle, bytes.Compare)
		pos += s.lowerByte[fb]
	} else {
		pos, exactMatch = slices.BinarySearchFunc(s.Index, needle, bytes.Compare)
	}
	if !exactMatch {
		if pos == 0 {
			// Try providing the first
			if bytes.HasPrefix(s.Index[0], needle) {
				if pos < len(s.Index)-1 {
					return pos, s.Index[0], s.Index[1], true
				}
				return 0, s.Index[0], nil, true
			}
			// If the record is before the first, give up
			return 0, nil, s.Index[0], false
		}
		// Go back one step
		pos--
	}
	if pos < len(s.Index)-1 {
		return pos, s.Index[pos], s.Index[pos+1], exactMatch
	}
	return pos, s.Index[pos], nil, exactMatch
}
