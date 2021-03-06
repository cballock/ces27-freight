package infra

import (
    "errors"
    "hash/crc32"
    "sort"
    "sync"
)

var ErrNodeNotFound = errors.New("node not found")

// HashRing is the data structure that holds a consistent hashed ring.
type HashRing struct {
    Nodes Nodes
    sync.Mutex
}

// search will find the index of the node that is responsible for the range that includes the hashed value of key.
func (r *HashRing) search(key string) int {
    hashId := hashId(key)
    i := 0
    // It's not necessary to lock/unlock the ring mutex in this method because the public methods that manipulate the ring structure (i.e., AddNode and RemoveNode) already do that;
    for nodeIndex, node := range r.Nodes {
        if hashId <= node.HashId { // Implementation note: this is valid because the nodes are increasinly ordered by their HashIds;
            i = nodeIndex
        }
    }
    return i
}

// NewHashRing will create a new HashRing object and return a pointer to it.
func NewHashRing() *HashRing {
    return &HashRing{Nodes: Nodes{}}
}

// Verify if a node with a given id already exists in the ring and if so return a pointer to it.
func (r *HashRing) Exists(id string) (bool, *Node) {
    r.Lock()
    defer r.Unlock()

    for _, node := range r.Nodes {
        if node.Id == id {
            return true, node
        }
    }

    return false, nil
}

// Add a node to the ring and return a pointer to it.
func (r *HashRing) AddNode(id string) *Node {
    r.Lock()
    defer r.Unlock()

    node := NewNode(id)
    r.Nodes = append(r.Nodes, node)

    sort.Sort(r.Nodes)

    return node
}

// Remove a node from the ring.
func (r *HashRing) RemoveNode(id string) error {
    r.Lock()
    defer r.Unlock()

    i := r.search(id)
    if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
        return ErrNodeNotFound
    }

    r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

    return nil
}

// Get the id of the node responsible for the hash range of id.
func (r *HashRing) Get(id string) string {
    i := r.search(id)
    if i >= r.Nodes.Len() {
        i = 0
    }

    return r.Nodes[i].Id
}

// GetNext will return the next node after the one responsible for the hash range of id.
func (r *HashRing) GetNext(id string) (string, error) {
    r.Lock()
    defer r.Unlock()
    var i = 0
    for i < r.Nodes.Len() && r.Nodes[i].Id != id {
        i++
    }

    if i >= r.Nodes.Len() {
        return "", ErrNodeNotFound
    }

    nextIndex := (i + 1) % r.Nodes.Len()

    return r.Nodes[nextIndex].Id, nil
}

// hashId returns the hashed form of a key.
func hashId(key string) uint32 {
    return crc32.ChecksumIEEE([]byte(key)) % uint32(1000)
}
