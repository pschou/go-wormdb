package wormdb

import (
	"bufio"
	"bytes"
	"container/list"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
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
	Finalize() error
}

type BinarySearch struct {
	Index                [][]byte
	list                 *list.List
	lowerByte, upperByte []int

	f    *os.File
	disk *DB
}

// Load a binary search from a memory 2-D byte slice.
func LoadBinarySearch(index [][]byte) *BinarySearch {
	bs := &BinarySearch{
		Index: index,
	}
	if Debug {
		log.Println("Loading binary search", len(index))
	}
	bs.makeFirstByte()
	return bs
}

// Load a binary search from disk.
func LoadDiskBinarySearch(file *os.File) (*BinarySearch, error) {
	bs := NewDiskBinarySearch(file)
	err := bs.LoadIndexToMemory()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// Build a search index in memory for the constructed wormdb.  Please note that
// there must be enough memory on the system for the Index when the database is
// being built.  This is in opposed to the [NewFileBinarySearch], which uses disk
// instead of memory.
func NewBinarySearch() *BinarySearch {
	return &BinarySearch{
		list: list.New(),
	}
}

// Build a search index on disk for the constructed wormdb.
// This is in opposed to the [NewMemoryBinarySearch], which uses memory
// instead of disk and is ready for use when Finalize() is called.
// One must then load the index from disk into memory with LoadIndexToMemory().
func NewDiskBinarySearch(file *os.File) *BinarySearch {
	db := &DB{
		file:      file,
		blocksize: 1 << 16, // 64k
		prev:      make([]byte, 0, 256),
	}
	db.offset = int64(db.offset / int64(db.blocksize))

	shift := 0
	for ; 1<<shift < db.blocksize; shift++ {
	}
	db.shift = shift
	db.blocksizeMask = int64(db.blocksize) - 1
	db.block = make([]byte, db.blocksize)
	db.writeBuf = bufio.NewWriterSize(file, int(db.blocksize))
	db.readpool = sync.Pool{New: func() interface{} { return make([]byte, db.blocksize) }}

	return &BinarySearch{
		disk: db,
	}
}

// When the index is built on disk, one can load the index into memory for use.
func (s *BinarySearch) LoadIndexToMemory() error {
	if len(s.Index) == 0 && s.disk != nil {
		list := list.New()
		err := s.disk.Walk(func(rec []byte) error {
			tmp := make([]byte, len(rec))
			copy(tmp, rec)
			list.PushBack(tmp)
			return nil
		})
		if err != nil {
			return err
		}
		s.Index = make([][]byte, list.Len())
		for i, e := 0, list.Front(); e != nil; i, e = i+1, e.Next() {
			s.Index[i] = e.Value.([]byte)
		}
		s.makeFirstByte()
	}
	return nil
}

// Add a record into the searchable list.  This involves an in-memory cache of
// the first record in each block and built using a link list so as to avoid
// growing memory and doing a slice copy.
//
// When Finalize() is called the linked list is flattened into a 2-D byte slice
// in memory and the list is disposed of.
func (s *BinarySearch) Add(needle []byte) error {
	if s.Index != nil {
		return fmt.Errorf("Could not add %q as search has been finalized", needle)
	}
	if s.list != nil {
		tmp := make([]byte, len(needle))
		copy(tmp, needle)
		s.list.PushBack(tmp)
		return nil
	}
	if s.disk != nil {
		err := s.disk.Add(needle)
		if err == nil {
			return nil
		}
		s.disk = nil
		return err
	}
	return fmt.Errorf("Could not add %q as no storage has been defined", needle)
}

// Do not call this directly, but instead wormdb calls this once the database
// has been finalized.
func (s *BinarySearch) Finalize() error {
	if s.list != nil {
		var list *list.List
		list, s.list = s.list, nil
		s.Index = make([][]byte, list.Len())
		for i, e := 0, list.Front(); e != nil; i, e = i+1, e.Next() {
			s.Index[i] = e.Value.([]byte)
		}
		s.makeFirstByte()
	}
	if s.disk != nil {
		return s.disk.Finalize()
	}
	return nil
}

// After a database has been loaded into memory from a save, call this to build
// the lower and upper byte bounds for faster searching capabilities.
func (s *BinarySearch) makeFirstByte() {
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
// or the lower and upper bound matches where the match would be located
// between two entries.  The purpose of the lower bound is to ensure that the
// match will be contained in the block retrieved from slow storage (such as a
// disk) and the upper bound is useful for segmenting data to make sure the
// result lies within the block.
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
