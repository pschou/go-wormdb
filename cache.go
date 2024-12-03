package wormdb

import (
	"log"
	"sync"

	"github.com/alphadose/haxmap"
)

type Cache interface {
	// Either get the value or begin to store the element into a cache
	GetOrCompute(key string, value func() *Result) (actual *Result, loaded bool)

	// Element being set is now fully populated, one can use this to delete older
	// elements as it is called once a Set has taken place.
	Stored(key string)
}

type CacheMap struct {
	_         noCopy
	lookupBuf *haxmap.Map[string, *Result]
	bufList   chan string
	bufMutex  sync.Mutex

	// Set this function to handle when a cached value is hit
	CountHit func(key string)
}

func NewCacheMap(size int) *CacheMap {
	c := &CacheMap{}
	if Debug {
		log.Println("Creating cache", size)
	}
	c.lookupBuf = haxmap.New[string, *Result]()
	c.bufList = make(chan string, size+10)
	return c
}

func (c *CacheMap) GetOrCompute(K string, V func() *Result) (r *Result, found bool) {
	r, found = c.lookupBuf.GetOrCompute(K, V)
	if found && c.CountHit != nil {
		c.CountHit(K)
	}
	return
}

func (c *CacheMap) Stored(K string) {
	// If the channel is filled, do some clearing
	if cap(c.bufList)-len(c.bufList) < 5 {
		c.bufMutex.Lock()
		for cap(c.bufList)-len(c.bufList) < 10 {
			c.lookupBuf.Del(<-c.bufList)
		}
		c.bufMutex.Unlock()
	}

	// Store our result needle for future fifo clearning
	c.bufList <- K
}
