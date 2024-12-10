package wormdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

// Turn on debug logging
var Debug bool

// noCopy implements sync.Locker so that go vet can trigger
// warnings when types embedding noCopy are copied.
type noCopy struct{}

func (c *noCopy) Lock()   {}
func (c *noCopy) Unlock() {}

type DB struct {
	_        noCopy
	file     *os.File
	offset   int64 // steps of blocksize
	shift    int   // must be in shift bits
	readpool sync.Pool

	// Writing functions (only available when newly created before finalize)
	prev          []byte
	writeBuf      *bufio.Writer
	written       int64
	blocksize     int
	blocksizeMask int64
	block         []byte

	// Lookup buffer
	cache  Cache
	search Search
}

type Option func(*DB)

// Include an optional cache to help with speeding up repeat calls.
func WithCache(c Cache) Option {
	return func(d *DB) {
		d.cache = c
	}
}

// Include a call back for the search function to use.  The built option uses
// the binary search to find records within the index.
func WithSearch(s Search) Option {
	return func(d *DB) {
		d.search = s
	}
}

// Offset at the beginning of the file to ignore.  This must be a step size of
// the blocksize.
func WithOffset(v int64) Option {
	return func(d *DB) {
		d.offset = v
	}
}

// Define a custom block size, if left unset the value of 4096 is used.
func WithBlockSize(v int) Option {
	return func(d *DB) {
		d.blocksize = v
	}
}

type Result struct {
	c   chan struct{}
	dat []byte
}

// Create a WORM db using the os.File handle to write a Write-Once-Read-Many
// ordered database optimized for reading based on sectors.
func New(file *os.File, options ...Option) (*DB, error) {
	db, err := Open(file, options...)
	if err != nil {
		return db, err
	}

	db.writeBuf = bufio.NewWriterSize(file, int(db.blocksize*8))

	return db, nil
}

// Open a wormdb for use, note that the index must be provided out of band.
func Open(file *os.File, options ...Option) (*DB, error) {
	db := &DB{
		file:      file,
		blocksize: 4096,
		prev:      make([]byte, 0, 256),
	}
	for _, o := range options {
		o(db)
	}

	// Make sure a search function is defined
	if db.search == nil {
		return nil, fmt.Errorf("Search method must be defined")
	}

	// Make sure the blocksize is a power of 2
	if db.blocksize < 256 || db.blocksize&(db.blocksize-1) != 0 {
		return nil, fmt.Errorf("Invalid block size %d.", db.blocksize)
	}

	// Make sure the offset is an interval of blocksize
	if db.offset%int64(db.blocksize) != 0 {
		return nil, fmt.Errorf("Offset %d must be a step of block size %d.", db.offset, db.blocksize)
	}
	db.offset = int64(db.offset / int64(db.blocksize))

	shift := 0
	for ; 1<<shift < db.blocksize; shift++ {
	}
	db.shift = shift
	db.blocksizeMask = int64(db.blocksize) - 1
	db.block = make([]byte, db.blocksize)
	db.readpool = sync.Pool{New: func() interface{} { return make([]byte, db.blocksize) }}

	return db, nil
}

// Search for a record in a wormdb and call func if a match is found.  Only the
// first matching prefix will be returned, so larger matches will be ignored.
//
// The slice MUST be copied to a local variable as the underlying byte slice
// will be reused in future function calls.
func (d DB) Get(needle []byte, handler func([]byte) error) error {
	var hasRec *Result
	// Do the cache check first to avoid walking or searching if a cache already exists
	if d.cache != nil {
		if Debug {
			log.Printf("Querying cache for %q", needle)
		}
		var ok bool
		hasRec, ok = d.cache.GetOrCompute(string(needle), func() *Result { return &Result{c: make(chan (struct{}))} })
		if ok {
			if Debug {
				log.Printf("Using cache for %q", needle)
			}
			if len(hasRec.dat) == 0 {
				<-hasRec.c // Ensure the record is ready for use (channel is closed)
			}
			if len(hasRec.dat) > 0 {
				// A record has been found!
				return handler(hasRec.dat)
			}
			// Buffered a failed to find entry record
			if Debug {
				log.Printf("No record cache for %q", needle)
			}
			return nil
		}
		defer close(hasRec.c)
		if Debug {
			log.Printf("Making cache for %q", needle)
		}
	}

	// Do the semi expensive search to find the sector on disk where the record should be located.
	n, first, matched := d.search.Find(needle)
	if matched {
		return handler(first)
	}
	if len(first) == 0 {
		// An error happened, first in index was not found. Do not continue.
		return nil
	}

	// Do the expensive part and read the sector from the disk where the record should be located.
	var b []byte
	{
		// Pull a buffer from the pool to read to
		buf := d.readpool.Get().([]byte)
		defer d.readpool.Put(buf)

		// Read the sector from disk where the record should be at
		rn, err := d.file.ReadAt(buf, (int64(n)+d.offset)<<d.shift)
		if err != nil && err != io.EOF {
			return err
		}

		// Trim down the result, this should only happen at the end of the file.
		b = buf[0:rn]
	}

	rec := make([]byte, 0, 256)

	for len(b) > 0 && b[0] > 0 {
		// The first in a sector contains the record length
		if len(b) <= int(b[0]) {
			return fmt.Errorf("Record too short at block %d", n)
		}
		rec = append(rec, b[1:int(b[0])+1]...)

		// Test if match is found
		if bytes.HasPrefix(rec, needle) {
			if hasRec != nil {
				if Debug {
					log.Printf("Storing cache for %q", needle)
				}
				// Create a copy in memory to store value
				tmp := make([]byte, len(rec))
				copy(tmp, rec)
				hasRec.dat = tmp

				d.cache.Stored(b2s(tmp[:len(needle)]))
			}
			return handler(rec)
		}
		// Trim off the record from the block
		b = b[b[0]+1:]
		if len(b) == 0 {
			return nil
		}

		// Determine the re-used portion of the record
		if len(rec) < int(b[0]) {
			return fmt.Errorf("Record prefix size too big at block %d", n)
		}
		rec = rec[:b[0]]
		b = b[1:]
		//return nil, nil
	}
	return nil
}

// Walk will return all the records in a wormdb.
//
// The slice MUST be copied to a local variable as the underlying byte slice
// will be reused in future function calls.
func (d DB) Walk(handler func([]byte) error) error {
	// Do the expensive part and read the sector from the disk where the record should be located.
	var (
		b    []byte
		done bool
		n    int
		rec  = make([]byte, 0, 256)
	)
	// Pull a buffer from the pool to read to
	buf := d.readpool.Get().([]byte)
	defer d.readpool.Put(buf)

	for !done {
		// Read the sector from disk where the record should be at
		rn, err := d.file.ReadAt(buf, (int64(n)+d.offset)<<d.shift)
		done = err == io.EOF
		if err != nil && err != io.EOF {
			return err
		}
		n++

		// Trim down the result, this should only happen at the end of the file.
		b = buf[0:rn]
		rec = rec[:0]

		for len(b) > 0 && b[0] > 0 {
			// The first in a sector contains the record length
			if len(b) <= int(b[0]) {
				return fmt.Errorf("Record too short at block %d", n)
			}
			rec = append(rec, b[1:int(b[0])+1]...)

			// Process record
			err = handler(rec)
			if err != nil {
				return err
			}

			// Trim off the record from the block
			b = b[b[0]+1:]
			if len(b) == 0 {
				// Done with this block, continue to next
				break
			}

			// Determine the re-used portion of the record
			if len(rec) < int(b[0]) {
				return fmt.Errorf("Record prefix size too big at block %d", n)
			}
			rec = rec[:b[0]]
			b = b[1:]
			//return nil, nil
		}
	}
	return nil
}

// Add a record to a wormdb when it is in write mode.
func (d *DB) Add(rec []byte) (err error) {
	if d.written&d.blocksizeMask == 0 {
		// Add the new block to the search index
		tmp := make([]byte, len(rec))
		copy(tmp, rec)
		d.search.Add(tmp)
	}

	// Handle first record case
	if d.written == 0 {
		d.writeBuf.WriteByte(byte(len(rec)))
		d.written++
		var n int
		n, err = d.writeBuf.Write(rec)
		d.written += int64(n)
		d.prev = append(d.prev, rec...)
		return
	}

	// Ensure ordering
	if bytes.Compare(d.prev, rec) >= 0 {
		return fmt.Errorf("Record %q cannot come after %q", rec, d.prev)
	}

	// Determine re-used bytes from previous record
	var reuse int
	for ; reuse < len(d.prev) && reuse < len(rec) && d.prev[reuse] == rec[reuse]; reuse++ {
	}

	// Check if space is available in current block
	avail := d.blocksize - int(d.written&d.blocksizeMask)
	if avail >= len(rec)-reuse+2 {
		d.writeBuf.WriteByte(byte(reuse))
		d.writeBuf.WriteByte(byte(len(rec) - reuse))
		d.written += 2
		var n int
		n, err = d.writeBuf.Write(rec[reuse:])
		d.written += int64(n)
		d.prev = d.prev[:0]
		d.prev = append(d.prev, rec...)
		return
	}

	d.written += int64(avail)
	d.writeBuf.Write(d.block[:avail])

	{
		// Add the new block to the search index
		tmp := make([]byte, len(rec))
		copy(tmp, rec)
		d.search.Add(tmp)
	}
	d.writeBuf.WriteByte(byte(len(rec)))
	d.written++
	var n int
	n, err = d.writeBuf.Write(rec)
	d.written += int64(n)
	d.prev = d.prev[:0]
	d.prev = append(d.prev, rec...)
	return
}

// Finalize the database, write any buffers to disk, and build search index.
func (d *DB) Finalize() (err error) {
	if d == nil {
		return nil
	}
	var wb *bufio.Writer
	wb, d.writeBuf = d.writeBuf, nil
	if wb != nil {
		if d.search != nil {
			d.search.Finalize()
		}
		err = wb.Flush()
		d.file.Sync()
	}
	return
}

// Close the database and the file handle at the same time.
func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	d.Finalize()
	return d.file.Close()
}
