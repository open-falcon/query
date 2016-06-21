package cache

/**
 * Provides cache functions for loading and cache expiration
 */
import (
	"github.com/patrickmn/go-cache"
)

// Defines the behavior of access on cache pool
// 1. The getter of key in cache pool
// 2. The loading of source data if object is not in cache
// 3. The setting of data to cache
type CacheWorker interface {
	// Get data of object which is missed in cache
	LoadSourceData() (interface{}, error)
	// Put loaded data of object into cache
	// 2nd parameter is the search
	SetCache(*cache.Cache, interface{})
}

// Callback function used to load data from data source if the cache is not existing
// Sets the cache pool and loading function
type CachePool struct {
	Cache *cache.Cache
}

// Gets data by key(or load it by provided function)
func (cachePool CachePool) Get(key string, cacheWorker CacheWorker) (interface{}, error) {
	cachedObject, hasCache := cachePool.Cache.Get(key)

	if !hasCache {
		var err error

		if cachedObject, err = cacheWorker.LoadSourceData()
			err != nil {
			return nil, err
		}

		cacheWorker.SetCache(cachePool.Cache, cachedObject)
	}

	cachedObject, _ = cachePool.Cache.Get(key)

	return cachedObject, nil
}
