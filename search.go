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

	// Called each time Get is called to look up a record and determine the sector
	// on disk to read from.
	Find(needle []byte) (sectorId int, closest []byte, wasExactMatch bool)

	// Called after last record has been added.
	Finalize()
}

type BinarySearch struct {
	Index [][]byte
	list  *list.List
}

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

func (s *BinarySearch) Finalize() {
	if s.list != nil {
		s.Index = make([][]byte, s.list.Len())
		for i, e := 0, s.list.Front(); e != nil; i, e = i+1, e.Next() {
			s.Index[i] = e.Value.([]byte)
		}
		s.list = nil
	}
}

func (s *BinarySearch) Find(needle []byte) (pos int, closest []byte, exactMatch bool) {
	pos, exactMatch = slices.BinarySearchFunc(s.Index, needle, bytes.Compare)
	//fmt.Println("binary search found", n, ok)
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
