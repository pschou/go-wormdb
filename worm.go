package wormdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync"

	"github.com/alphadose/haxmap"
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
	Index    [][]byte
	file     *os.File
	offset   int64 // steps of blocksize
	shift    int   // must be in shift bits
	readpool sync.Pool

	// Writing functions (only available when newly created before finalize)
	prev     []byte
	writeBuf *bufio.Writer

	// Lookup buffer
	lookupBuf *haxmap.Map[string, *result]
	bufList   chan string
	bufMutex  sync.Mutex
}

type result struct {
	c   chan struct{}
	dat []byte
}

func (d *DB) InitCache(size int) {
	if Debug {
		log.Println("Creating cache", size)
	}
	d.lookupBuf = haxmap.New[string, *result]()
	d.bufList = make(chan string, size+10)
}

// Create a WORM db using the os.File handle to write a Write-Once-Read-Many
// ordered database optimized for reading based on sectors.
func New(file *os.File, offset, blocksize int) (*DB, error) {
	// Make sure the blocksize is a power of 2
	if blocksize >= 256 && blocksize&(blocksize-1) != 0 {
		return nil, fmt.Errorf("Invalid block size %d.", blocksize)
	}
	// Make sure the offset is an interval of blocksize
	if offset%blocksize != 0 {
		return nil, fmt.Errorf("Offset %d must be a step of block size %d.", offset, blocksize)
	}
	shift := 0
	for ; 1<<shift < blocksize; shift++ {
		//fmt.Println("shift:", shift)
	}
	//fmt.Println("bs", blocksize, "offset", offset, "shift", shift)
	return &DB{
		file:     file,
		offset:   int64(offset / blocksize),
		shift:    shift,
		readpool: sync.Pool{New: func() interface{} { return make([]byte, blocksize) }},
		writeBuf: bufio.NewWriterSize(file, blocksize),
		prev:     make([]byte, 0, 256),
	}, nil
}

// Open a wormdb for use, note that the index must be provided out of band.
func Open(file *os.File, offset, blocksize int64) (*DB, error) {
	// Make sure the blocksize is a power of 2
	if blocksize >= 256 && blocksize&(blocksize-1) != 0 {
		return nil, fmt.Errorf("Invalid block size %d.", blocksize)
	}
	// Make sure the offset is an interval of blocksize
	if offset%blocksize != 0 {
		return nil, fmt.Errorf("Offset %d must be a step of block size %d.", offset, blocksize)
	}
	shift := 0
	for ; 1<<shift < blocksize; shift++ {
		//fmt.Println("shift:", shift)
	}
	//fmt.Println("bs", blocksize, "offset", offset, "shift", shift)
	return &DB{
		file:     file,
		offset:   offset / blocksize,
		shift:    shift,
		readpool: sync.Pool{New: func() interface{} { return make([]byte, blocksize) }},
	}, nil
}

// Search for a record in a wormdb and call func if a match is found.  Only the
// first matching prefix will be returned, so larger matches will be ignored.
//
// The slice MUST be copied to a local variable as the underlying byte slice
// will be reused in future function calls.
func (d DB) Get(needle []byte, handler func([]byte) error) error {
	n, ok := slices.BinarySearchFunc(d.Index, needle, bytes.Compare)
	//fmt.Println("binary search found", n, ok)
	if !ok {
		if n == 0 {
			// Try providing the first
			if bytes.HasPrefix(d.Index[0], needle) {
				return handler(d.Index[0])
			}
			// If the record is before the first, give up
			return nil
		}
		// Go back one step
		n--
	}

	var haxRec *result
	if d.lookupBuf != nil {
		if Debug {
			log.Printf("Querying cache for %q", needle)
		}
		var (
			myChan = make(chan (struct{}))
			ok     bool
		)
		defer close(myChan)
		haxRec = &result{c: myChan}
		haxRec, ok = d.lookupBuf.GetOrSet(string(needle), haxRec)
		if ok {
			if Debug {
				log.Println("using buffer")
			}
			if len(haxRec.dat) == 0 {
				<-haxRec.c // Ensure the record is ready for use (channel is closed)
			}
			if len(haxRec.dat) > 0 {
				// A record has been found!
				return handler(haxRec.dat)
			}
			// Buffered a failed to find entry record
			return nil
		}
		if Debug {
			log.Println("making buffer")
		}
	}

	var b []byte
	{
		// Pull a buffer from the pool to read to
		buf := d.readpool.Get().([]byte)
		defer d.readpool.Put(buf)
		//fmt.Println("reading at", (int64(n)+d.offset)<<d.shift)
		rn, err := d.file.ReadAt(buf, (int64(n)+d.offset)<<d.shift)
		//fmt.Println("reading bytes", rn)
		if err != nil && err != io.EOF {
			return err
		}
		b = buf[0:rn]
		//fmt.Printf("read %q\n", b)
	}

	rec := make([]byte, 0, 256)

	for len(b) > 0 && b[0] > 0 {
		//fmt.Println("len b", len(b), "b0", b[0])
		if len(b) <= int(b[0]) {
			return fmt.Errorf("Record too short block %d", n)
		}
		rec = append(rec, b[1:int(b[0])+1]...)
		//fmt.Printf("rec %q   bslice %q\n", rec, b[1:(int(b[0])+1)])

		// Test if match is found
		if bytes.HasPrefix(rec, needle) {
			if haxRec != nil {
				// Create a copy in memory to store value
				tmp := make([]byte, len(rec))
				copy(tmp, rec)
				haxRec.dat = tmp

				// If the channel is filled, do some clearing
				if cap(d.bufList)-len(d.bufList) < 5 {
					d.bufMutex.Lock()
					for cap(d.bufList)-len(d.bufList) < 10 {
						d.lookupBuf.Del(<-d.bufList)
					}
					d.bufMutex.Unlock()
				}

				// Store our result needle for future fifo clearning
				d.bufList <- b2s(tmp[:len(needle)])
			}
			return handler(rec)
		}
		// Trim off the record from the block
		b = b[b[0]+1:]
		if len(b) == 0 {
			return nil
		}
		//fmt.Printf("b %q\n", b)
		// Determine the re-used portion of the record
		rec = rec[:b[0]]
		//fmt.Printf("trimmed rec %q\n", rec)
		b = b[1:]
		//fmt.Printf("loop b %q\n", b)
		//return nil, nil
	}
	return nil
}

// Add a record to a wormdb when it is in write mode.
func (d *DB) Add(rec []byte) (err error) {
	// Handle first record case
	if len(d.Index) == 0 {
		tmp := make([]byte, len(rec))
		copy(tmp, rec)
		d.Index = append(d.Index, tmp)
		d.writeBuf.WriteByte(byte(len(rec)))
		//fmt.Println("writing to buf", string(rec))
		_, err = d.writeBuf.Write(rec)
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
	//curSize := d.writeBuf.Buffered()
	//fmt.Println("curSize=", curSize, "avail=", d.writeBuf.Available())
	if d.writeBuf.Available() >= len(rec)-reuse+2 {
		d.writeBuf.WriteByte(byte(reuse))
		d.writeBuf.WriteByte(byte(len(rec) - reuse))
		_, err = d.writeBuf.Write(rec[reuse:])
		d.prev = d.prev[:0]
		d.prev = append(d.prev, rec...)
		return
	}

	for i := d.writeBuf.Available(); i > 0; i-- {
		//fmt.Println("padding 0")
		d.writeBuf.WriteByte(0)
	}

	tmp := make([]byte, len(rec))
	copy(tmp, rec)
	d.Index = append(d.Index, tmp)
	//fmt.Printf("index = %q\n", d.Index)
	d.writeBuf.WriteByte(byte(len(rec)))
	_, err = d.writeBuf.Write(rec)
	d.prev = d.prev[:0]
	d.prev = append(d.prev, rec...)
	return
}

// Finalize the database write mode and switch to read mode.
func (d DB) Finalize() (err error) {
	if d.writeBuf != nil {
		for i := d.writeBuf.Available(); i > 0; i-- {
			//fmt.Println("padding 0")
			d.writeBuf.WriteByte(0)
		}
		err = d.writeBuf.Flush()
		d.writeBuf = nil
	}
	return
}

// Close the database and the file handle at the same time.
func (d DB) Close() error {
	d.Finalize()
	return d.file.Close()
}
