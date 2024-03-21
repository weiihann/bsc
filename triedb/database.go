// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package triedb

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/ethereum/go-ethereum/triedb/database"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

// Config defines all necessary options for database.
type Config struct {
	Preimages bool // Flag whether the preimage of node key is recorded
	Cache     int
	NoTries   bool
	IsVerkle  bool           // Flag whether the db is holding a verkle tree
	HashDB    *hashdb.Config // Configs for hash-based scheme
	PathDB    *pathdb.Config // Configs for experimental path-based scheme
}

// HashDefaults represents a config for using hash-based scheme with
// default settings.
var HashDefaults = &Config{
	Preimages: false,
	HashDB:    hashdb.Defaults,
}

// backend defines the methods needed to access/update trie nodes in different
// state scheme.
type backend interface {
	// Scheme returns the identifier of used storage scheme.
	Scheme() string

	// Initialized returns an indicator if the state data is already initialized
	// according to the state scheme.
	Initialized(genesisRoot common.Hash) bool

	// Size returns the current storage size of the memory cache in front of the
	// persistent database layer.
	Size() (common.StorageSize, common.StorageSize, common.StorageSize)

	// Update performs a state transition by committing dirty nodes contained
	// in the given set in order to update state from the specified parent to
	// the specified root.
	//
	// The passed in maps(nodes, states) will be retained to avoid copying
	// everything. Therefore, these maps must not be changed afterwards.
	Update(root common.Hash, parent common.Hash, block uint64, nodes *trienode.MergedNodeSet, states *triestate.Set) error

	// Commit writes all relevant trie nodes belonging to the specified state
	// to disk. Report specifies whether logs will be displayed in info level.
	Commit(root common.Hash, report bool) error

	// Close closes the trie database backend and releases all held resources.
	Close() error
}

// Database is the wrapper of the underlying backend which is shared by different
// types of node backend as an entrypoint. It's responsible for all interactions
// relevant with trie nodes and node preimages.
type Database struct {
	config    *Config        // Configuration for trie database
	diskdb    ethdb.Database // Persistent database to store the snapshot
	preimages *preimageStore // The store for caching preimages
	backend   backend        // The backend for managing trie nodes
}

// NewDatabase initializes the trie database with default settings, note
// the legacy hash-based scheme is used by default.
func NewDatabase(diskdb ethdb.Database, config *Config) *Database {
	// Sanitize the config and use the default one if it's not specified.
	var triediskdb ethdb.Database
	if diskdb != nil && diskdb.StateStore() != nil {
		triediskdb = diskdb.StateStore()
	} else {
		triediskdb = diskdb
	}
	dbScheme := rawdb.ReadStateScheme(diskdb)
	if config == nil {
		if dbScheme == rawdb.PathScheme {
			config = &Config{
				PathDB: pathdb.Defaults,
			}
		} else {
			config = HashDefaults
		}
	}
	if config.PathDB == nil && config.HashDB == nil {
		if dbScheme == rawdb.PathScheme {
			config.PathDB = pathdb.Defaults
		} else {
			config.HashDB = hashdb.Defaults
		}
	}
	var preimages *preimageStore
	if config.Preimages {
		preimages = newPreimageStore(triediskdb)
	}
	db := &Database{
		config:    config,
		diskdb:    triediskdb,
		preimages: preimages,
	}
	/*
	 * 1. First, initialize db according to the user config
	 * 2. Second, initialize the db according to the scheme already used by db
	 * 3. Last, use the default scheme, namely hash scheme
	 */
	if config.HashDB != nil {
		if rawdb.ReadStateScheme(triediskdb) == rawdb.PathScheme {
			log.Warn("Incompatible state scheme", "old", rawdb.PathScheme, "new", rawdb.HashScheme)
		}
		db.backend = hashdb.New(triediskdb, config.HashDB, trie.MerkleResolver{})
	} else if config.PathDB != nil {
		if rawdb.ReadStateScheme(triediskdb) == rawdb.HashScheme {
			log.Warn("Incompatible state scheme", "old", rawdb.HashScheme, "new", rawdb.PathScheme)
		}
		db.backend = pathdb.New(triediskdb, config.PathDB)
	} else if strings.Compare(dbScheme, rawdb.PathScheme) == 0 {
		if config.PathDB == nil {
			config.PathDB = pathdb.Defaults
		}
		db.backend = pathdb.New(triediskdb, config.PathDB)
	} else {
		var resolver hashdb.ChildResolver
		if config.IsVerkle {
			// TODO define verkle resolver
			log.Crit("Verkle node resolver is not defined")
		} else {
			resolver = trie.MerkleResolver{}
		}
		if config.HashDB == nil {
			config.HashDB = hashdb.Defaults
		}
		db.backend = hashdb.New(triediskdb, config.HashDB, resolver)
	}
	return db
}

func (db *Database) Config() *Config {
	return db.config
}

func (db *Database) DiskDB() ethdb.Database {
	return db.diskdb
}

// Reader returns a reader for accessing all trie nodes with provided state root.
// An error will be returned if the requested state is not available.
func (db *Database) Reader(blockRoot common.Hash) (database.Reader, error) {
	switch b := db.backend.(type) {
	case *hashdb.Database:
		return b.Reader(blockRoot)
	case *pathdb.Database:
		return b.Reader(blockRoot)
	}
	return nil, errors.New("unknown backend")
}

// Update performs a state transition by committing dirty nodes contained in the
// given set in order to update state from the specified parent to the specified
// root. The held pre-images accumulated up to this point will be flushed in case
// the size exceeds the threshold.
//
// The passed in maps(nodes, states) will be retained to avoid copying everything.
// Therefore, these maps must not be changed afterwards.
func (db *Database) Update(root common.Hash, parent common.Hash, block uint64, nodes *trienode.MergedNodeSet, states *triestate.Set) error {
	if db.preimages != nil {
		db.preimages.commit(false)
	}
	return db.backend.Update(root, parent, block, nodes, states)
}

// Commit iterates over all the children of a particular node, writes them out
// to disk. As a side effect, all pre-images accumulated up to this point are
// also written.
func (db *Database) Commit(root common.Hash, report bool) error {
	if db.preimages != nil {
		db.preimages.commit(true)
	}
	return db.backend.Commit(root, report)
}

// Size returns the storage size of diff layer nodes above the persistent disk
// layer, the dirty nodes buffered within the disk layer, and the size of cached
// preimages.
func (db *Database) Size() (common.StorageSize, common.StorageSize, common.StorageSize, common.StorageSize) {
	var (
		diffs, nodes, immutablenodes common.StorageSize
		preimages                    common.StorageSize
	)
	diffs, nodes, immutablenodes = db.backend.Size()
	if db.preimages != nil {
		preimages = db.preimages.size()
	}
	return diffs, nodes, immutablenodes, preimages
}

// Initialized returns an indicator if the state data is already initialized
// according to the state scheme.
func (db *Database) Initialized(genesisRoot common.Hash) bool {
	return db.backend.Initialized(genesisRoot)
}

// Scheme returns the node scheme used in the database.
func (db *Database) Scheme() string {
	return db.backend.Scheme()
}

// Close flushes the dangling preimages to disk and closes the trie database.
// It is meant to be called when closing the blockchain object, so that all
// resources held can be released correctly.
func (db *Database) Close() error {
	db.WritePreimages()
	return db.backend.Close()
}

// WritePreimages flushes all accumulated preimages to disk forcibly.
func (db *Database) WritePreimages() {
	if db.preimages != nil {
		db.preimages.commit(true)
	}
}

// Preimage retrieves a cached trie node pre-image from preimage store.
func (db *Database) Preimage(hash common.Hash) []byte {
	if db.preimages == nil {
		return nil
	}
	return db.preimages.preimage(hash)
}

// InsertPreimage writes pre-images of trie node to the preimage store.
func (db *Database) InsertPreimage(preimages map[common.Hash][]byte) {
	if db.preimages == nil {
		return
	}
	db.preimages.insertPreimage(preimages)
}

// Cap iteratively flushes old but still referenced trie nodes until the total
// memory usage goes below the given threshold. The held pre-images accumulated
// up to this point will be flushed in case the size exceeds the threshold.
//
// It's only supported by hash-based database and will return an error for others.
func (db *Database) Cap(limit common.StorageSize) error {
	hdb, ok := db.backend.(*hashdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	if db.preimages != nil {
		db.preimages.commit(false)
	}
	return hdb.Cap(limit)
}

// Reference adds a new reference from a parent node to a child node. This function
// is used to add reference between internal trie node and external node(e.g. storage
// trie root), all internal trie nodes are referenced together by database itself.
//
// It's only supported by hash-based database and will return an error for others.
func (db *Database) Reference(root common.Hash, parent common.Hash) error {
	hdb, ok := db.backend.(*hashdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	hdb.Reference(root, parent)
	return nil
}

// Dereference removes an existing reference from a root node. It's only
// supported by hash-based database and will return an error for others.
func (db *Database) Dereference(root common.Hash) error {
	hdb, ok := db.backend.(*hashdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	hdb.Dereference(root)
	return nil
}

// Recover rollbacks the database to a specified historical point. The state is
// supported as the rollback destination only if it's canonical state and the
// corresponding trie histories are existent. It's only supported by path-based
// database and will return an error for others.
func (db *Database) Recover(target common.Hash) error {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	var loader triestate.TrieLoader
	if db.config.IsVerkle {
		// TODO define verkle loader
		log.Crit("Verkle loader is not defined")
	} else {
		loader = trie.NewMerkleLoader(db)
	}
	return pdb.Recover(target, loader)
}

// Recoverable returns the indicator if the specified state is enabled to be
// recovered. It's only supported by path-based database and will return an
// error for others.
func (db *Database) Recoverable(root common.Hash) (bool, error) {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return false, errors.New("not supported")
	}
	return pdb.Recoverable(root), nil
}

// Disable deactivates the database and invalidates all available state layers
// as stale to prevent access to the persistent state, which is in the syncing
// stage.
//
// It's only supported by path-based database and will return an error for others.
func (db *Database) Disable() error {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	return pdb.Disable()
}

// Enable activates database and resets the state tree with the provided persistent
// state root once the state sync is finished.
func (db *Database) Enable(root common.Hash) error {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	return pdb.Enable(root)
}

// Journal commits an entire diff hierarchy to disk into a single journal entry.
// This is meant to be used during shutdown to persist the snapshot without
// flattening everything down (bad for reorgs). It's only supported by path-based
// database and will return an error for others.
func (db *Database) Journal(root common.Hash) error {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	return pdb.Journal(root)
}

// SetBufferSize sets the node buffer size to the provided value(in bytes).
// It's only supported by path-based database and will return an error for
// others.
func (db *Database) SetBufferSize(size int) error {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return errors.New("not supported")
	}
	return pdb.SetBufferSize(size)
}

// Head return the top non-fork difflayer/disklayer root hash for rewinding.
// It's only supported by path-based database and will return empty hash for
// others.
func (db *Database) Head() common.Hash {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		return common.Hash{}
	}
	return pdb.Head()
}

// GetAllHash returns all MPT root hash in diffLayer and diskLayer.
// It's only supported by path-based database and will return nil for
// others.
func (db *Database) GetAllRooHash() [][]string {
	pdb, ok := db.backend.(*pathdb.Database)
	if !ok {
		log.Error("Not supported")
		return nil
	}
	return pdb.GetAllRooHash()
}

// IsVerkle returns the indicator if the database is holding a verkle tree.
func (db *Database) IsVerkle() bool {
	return db.config.IsVerkle
}
