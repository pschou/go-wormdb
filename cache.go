package wormdb

import (
	"log"
	"sync"

	"github.com/alphadose/haxmap"
)

type Cache interface {
	// Either get the value or begin to store the element into a cache
	GetOrSet(key string, value *Result) (actual *Result, loaded bool)

	// Element being set is now fully populated, one can use this to delete older
	// elements as it is called once a Set has taken place.
	Stored(key string)
}

type CacheMap struct {
	_         noCopy
	lookupBuf *haxmap.Map[string, *Result]
	bufList   chan string
	bufMutex  sync.Mutex
}

func NewCacheMap(size int) Cache {
	c := &CacheMap{}
	if Debug {
		log.Println("Creating cache", size)
	}
	c.lookupBuf = haxmap.New[string, *Result]()
	c.bufList = make(chan string, size+10)
	return c
}

func (c *CacheMap) GetOrSet(K string, V *Result) (*Result, bool) {
	return c.lookupBuf.GetOrSet(K, V)
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
