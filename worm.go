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

	old  *Walker     // When merging, this field is set to the old DB.
	comp CompareFunc // Comparison function for merging records together.

	// Lookup buffer
	cache  Cache
	search Search
}

type Walker struct {
	db     *DB    // Pointer to underlying database
	done   bool   // Done reading.
	atEOF  bool   // End of file hit.
	rec    []byte // Current record handle.
	n      int64  // Current block in database
	b, buf []byte // Buffer for reading from file
	err    error  // Error holding from last read
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

// Compare returns an integer comparing two byte slices lexicographically. The
// result will be 0 if a == b, -1 if a < b, and +1 if a > b. A nil argument is
// equivalent to an empty slice.
//
// Record removal is also possible, if -2 is provided then only `a` will be
// used and if +2 is provided then only `b` will be used.
//
// This is mainly used with the WithMerge function. Note that the previous
// merged-to database will always be `a` and the incoming data will be `b`.
type CompareFunc func(a, b []byte) int

// Build from a previous wormDB and merge the records.
func WithMerge(old *DB, comp CompareFunc) Option {
	return func(d *DB) {
		d.old = old.NewWalker()
		d.comp = comp
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
	if d.search == nil {
		return fmt.Errorf("No search method defined for finding %q", needle)
	}
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

// NewWalker will return all the records in a wormdb with a scanner like interface.
//
// The slice MUST be copied to a local variable as the underlying byte slice
// will be reused in future function calls.
func (d *DB) NewWalker() *Walker {
	return &Walker{rec: make([]byte, 0, 256), db: d, n: d.offset}
}

// Err returns the first non-EOF error that was encountered by the [Walker].
func (w *Walker) Err() error {
	if w.err == io.EOF {
		return nil
	}
	return w.err
}

// Bytes returns the most recent token generated by a call to [Walker.Scan].
// The underlying array may point to data that will be overwritten
// by a subsequent call to Scan. It does no allocation.
func (w *Walker) Bytes() []byte {
	return w.rec
}

// Text returns the most recent token generated by a call to [Walker.Scan]
// as a newly allocated string holding its bytes.
func (s *Walker) Text() string {
	return string(s.rec)
}

// Scan advances the [Walker] to the next token, which will then be
// available through the [Walker.Bytes] or [Walker.Text] method. It returns false when
// there are no more tokens, either by reaching the end of the input or an error.
// After Scan returns false, the [Walker.Err] method will return any error that
// occurred during scanning, except that if it was [io.EOF], [Walker.Err]
// will return nil.
func (w *Walker) Scan() bool {
	if w.done {
		return false
	}
	// Pull a buffer from the pool to read to do the expensive part and read the
	// sector from the disk where the record should be located.
	if w.buf == nil {
		w.buf = w.db.readpool.Get().([]byte)
		//defer d.readpool.Put(buf)
	}

	if len(w.b) > 0 {
		// Determine the re-used portion of the record
		if len(w.b) < 2 || len(w.rec) < int(w.b[0]) {
			w.err = fmt.Errorf("Bad record prefix at block %d", w.n)
			w.done, w.rec = true, nil
			return false
		}
		w.rec = w.rec[:w.b[0]]
		w.b = w.b[1:]

		if w.b[0] > 0 {
			if len(w.b) < int(w.b[0])+1 {
				w.err = fmt.Errorf("Bad record size at block %d", w.n)
				w.done, w.rec = true, nil
				return false
			}
			w.rec = append(w.rec, w.b[1:int(w.b[0])+1]...)

			// Trim off the record from the block
			w.b = w.b[w.b[0]+1:]
			return true
		} else {
			// Truncate to prepare for next record
			w.b = nil
		}
	}

	if w.atEOF {
		w.done, w.rec = true, nil
		return false
	}

	// Proceed to read the next block when nothing is left of the current block
	// or the next record size is 0, the indicator that the block is complete.

	// Read the sector from disk where the record should be at
	rn, err := w.db.file.ReadAt(w.buf, (int64(w.n)+w.db.offset)<<w.db.shift)
	w.atEOF = err == io.EOF
	if err != nil && err != io.EOF {
		w.err = err
		w.db.readpool.Put(w.buf)
		return false
	}
	// Trim down the result, this should only happen at the end of the file.
	w.b = w.buf[0:rn]

	// The first byte in a block contains the record length
	if len(w.b) == 0 || len(w.b) <= int(w.b[0])+1 {
		w.err = fmt.Errorf("Record too short at block %d", w.n)
		w.done, w.rec = true, nil
		return false
	}
	w.n++

	// First record in block is always a full record
	w.rec = w.rec[:0]
	w.rec = append(w.rec, w.b[1:int(w.b[0])+1]...)

	// Trim off the record from the block
	w.b = w.b[w.b[0]+1:]

	return true
}

// Add a record to a wormdb when it is in write mode.
func (d *DB) Add(rec []byte) (err error) {
	if d.old == nil {
		// Simple case where records have not already been read
		return d.add(rec)
	}
	if len(d.old.rec) == 0 {
		// Start the walk
		if !d.old.Scan() {
			// At the end
			d.old = nil
			return d.add(rec)
		}
	}

	for todo := true; todo; {
		x := d.comp(d.old.rec, rec)
		switch x {
		case -2: // A is wanted more, so it goes first and B is ignored
			if err := d.add(d.old.rec); err != nil {
				return err
			}
			d.old.Scan()
			return d.old.Err()
		case -1, 0: // A is less, so it goes first
			if err := d.add(d.old.rec); err != nil {
				return err
			}
			todo = d.old.Scan()
		case 1: // B is less, so it goes first
			return d.add(rec)
		case 2: // B is wanted more, so it goes first and A is ignored
			d.old.Scan()
			return d.add(rec)
		}
	}
	return d.add(rec)
}

func (d *DB) add(rec []byte) (err error) {
	if d.written&d.blocksizeMask == 0 {
		// Add the new block to the search index
		if d.search != nil {
			d.search.Add(rec)
		}
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

	// Add the new block to the search index
	if d.search != nil {
		d.search.Add(rec)
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
	if d.old != nil {
		if len(d.old.rec) > 0 {
			d.add(d.old.rec)
		}
		for d.old.Scan() {
			d.add(d.old.rec)
		}
		d.old = nil
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
	d.search = nil // Make sure memory is no longer referenced here.
	return d.file.Close()
}
