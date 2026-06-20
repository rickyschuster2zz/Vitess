package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Errors
var (
	ErrShardNotServing = errors.New("shard is not serving")
	ErrStaleRead       = errors.New("stale read detected")
)

// Shard represents a database shard.
type Shard struct {
	Name    string
	Serving bool
	Data    string
}

// TopologyServer represents the global topology service (e.g., etcd/ZooKeeper).
type TopologyServer struct {
	mu      sync.RWMutex
	Routing map[string]string // keyspace -> shard name
	Shards  map[string]*Shard
	Version int64
}

func NewTopologyServer() *TopologyServer {
	return &TopologyServer{
		Routing: make(map[string]string),
		Shards:  make(map[string]*Shard),
	}
}

func (ts *TopologyServer) GetSrvKeyspace(keyspace string) (string, int64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	shardName, ok := ts.Routing[keyspace]
	if !ok {
		return "", 0, fmt.Errorf("keyspace %s not found", keyspace)
	}
	return shardName, ts.Version, nil
}

func (ts *TopologyServer) UpdateRouting(keyspace string, targetShard string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Routing[keyspace] = targetShard
	ts.Version++
}

// SrvKeyspace represents the cached routing info in VTGate.
type SrvKeyspace struct {
	ShardName string
	Version   int64
}

// VTGate represents the gateway routing queries.
type VTGate struct {
	topo *TopologyServer

	mu    sync.RWMutex
	cache map[string]*SrvKeyspace
}

func NewVTGate(topo *TopologyServer) *VTGate {
	return &VTGate{
		topo:  topo,
		cache: make(map[string]*SrvKeyspace),
	}
}

// GetSrvKeyspace gets the cached routing or fetches it from topo if not present.
func (v *VTGate) GetSrvKeyspace(keyspace string) (*SrvKeyspace, error) {
	v.mu.RLock()
	cached, ok := v.cache[keyspace]
	v.mu.RUnlock()

	if ok {
		return cached, nil
	}

	// Cache miss, fetch from topo
	return v.refreshCache(keyspace)
}

func (v *VTGate) refreshCache(keyspace string) (*SrvKeyspace, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	shardName, version, err := v.topo.GetSrvKeyspace(keyspace)
	if err != nil {
		return nil, err
	}

	sk := &SrvKeyspace{
		ShardName: shardName,
		Version:   version,
	}
	v.cache[keyspace] = sk
	return sk, nil
}

func (v *VTGate) invalidateCache(keyspace string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.cache, keyspace)
}

// Execute routes and executes a query.
func (v *VTGate) Execute(keyspace string, query string) (string, error) {
	// 1. Resolve routing
	sk, err := v.GetSrvKeyspace(keyspace)
	if err != nil {
		return "", err
	}

	// 2. Attempt execution on the resolved shard
	v.topo.mu.RLock()
	shard, ok := v.topo.Shards[sk.ShardName]
	v.topo.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("shard %s not found", sk.