package wormdb

import (
	"container/list"
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
	bufList   *list.List
	bufMutex  sync.Mutex
	max       int

	// Set this function to handle when a cached value is hit
	CountHit func(key string)
}

func NewCacheMap(size int) *CacheMap {
	if Debug {
		log.Println("Creating cache", size)
	}
	return &CacheMap{
		max:       size,
		lookupBuf: haxmap.New[string, *Result](),
		bufList:   list.New(),
	}
}

func (c *CacheMap) GetOrCompute(K string, V func() *Result) (*Result, bool) {
	myElm, found := c.lookupBuf.GetOrCompute(K, func() *Result {
		val := V()

		// Fork off the list amendment action
		go func(K string) {
			// Need to lock due to container.list not being thread safe!
			c.bufMutex.Lock()
			defer c.bufMutex.Unlock()

			// Push the value to the end of the list
			c.bufList.PushBack(K)

			// If the list is too long, pop off the front and flush it
			if int(c.bufList.Len()) > c.max {
				if val, ok := c.bufList.Remove(c.bufList.Front()).(string); ok {
					c.lookupBuf.Del(val)
				}
			}
		}(K)

		// Return the created value
		return val
	})

	if found && c.CountHit != nil {
		c.CountHit(K)
	}

	return myElm, found
}

func (c *CacheMap) Stored(K string) {
}
