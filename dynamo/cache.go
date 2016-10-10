package dynamo

import (
    "log"
    "sync"
)

///////////////////////////////
// THIS FILE IS MISSING CODE //
///////////////////////////////
type TimedValue struct {
    value string
    timestamp int64
}

// Cache is the struct that handle all the data storage for the dynamo server.
type Cache struct {
    data map[string]TimedValue
    sync.Mutex
}

// Create a new cache object and return a pointer to it.
func NewCache() *Cache {
    var s Cache

    s.data = make(map[string]TimedValue)

    return &s
}

// Get the value of a key from the storage. This will handle concurrent get
// requests by locking the structure.
func (cache *Cache) Get(key string) (value string, timestamp int64) {
    cache.Lock()
    timedValue := cache.data[key]
    value = timedValue.value
    timestamp = timedValue.timestamp
    cache.Unlock()

    log.Printf("[CACHE] Getting Key '%v' with Value '%v' @ timestamp '%v'\n", key, value, timestamp)
    return
}

// Put a value to a key in the storage. This will handle concurrent put
// requests by locking the structure.
func (cache *Cache) Put(key string, value string, timestamp int64) {
    log.Printf("[CACHE] Putting Key '%v' with Value '%v' @ timestamp '%v'\n", key, value, timestamp)

    cache.Lock()
    cache.data[key] = TimedValue{value: value, timestamp: timestamp} // Assuming the new timestamp is always greater than the previous timestamps;
    cache.Unlock()

    return
}

// Retrieve all information from the server. This shouldn't be used in any way
// except for testing purposes.
func (cache *Cache) getAll() (data map[string]string, timestamps map[string]int64) {
    data = make(map[string]string)
    timestamps = make(map[string]int64)
    for key, timedValue := range cache.data {
        data[key] = timedValue.value
        timestamps[key] = timedValue.timestamp
    }
    return data, timestamps
}
