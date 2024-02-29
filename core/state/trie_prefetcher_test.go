// Copyright 2021 The go-ethereum Authors
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

package state

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
)

func filledStateDB() *StateDB {
	state, _ := New(types.EmptyRootHash, NewDatabase(rawdb.NewMemoryDatabase()), nil, 0, nil)

	// Create an account and check if the retrieved balance is correct
	addr := common.HexToAddress("0xaffeaffeaffeaffeaffeaffeaffeaffeaffeaffe")
	skey := common.HexToHash("aaa")
	sval := common.HexToHash("bbb")

	state.SetBalance(addr, big.NewInt(42)) // Change the account trie
	state.SetCode(addr, []byte("hello"))   // Change an external metadata
	state.SetState(addr, skey, sval)       // Change the storage trie
	for i := 0; i < 100; i++ {
		sk := common.BigToHash(big.NewInt(int64(i)))
		state.SetState(addr, sk, sk) // Change the storage trie
	}
	return state
}

func prefetchGuaranteed(prefetcher *triePrefetcher, owner common.Hash, root common.Hash, addr common.Address, keys [][]byte) {
	prefetcher.prefetch(owner, root, addr, keys)
	for {
		if len(prefetcher.prefetchChan) == 0 {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func TestCopyAndClose(t *testing.T) {
	db := filledStateDB()
	prefetcher := newTriePrefetcher(db.db, db.originalRoot, common.Hash{}, "")
	skey := common.HexToHash("aaa")
	prefetchGuaranteed(prefetcher, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	prefetchGuaranteed(prefetcher, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	time.Sleep(1 * time.Second)
	a := prefetcher.trie(common.Hash{}, db.originalRoot)
	prefetchGuaranteed(prefetcher, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	b := prefetcher.trie(common.Hash{}, db.originalRoot)
	cpy := prefetcher.copy()
	prefetchGuaranteed(cpy, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	prefetchGuaranteed(cpy, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	c := cpy.trie(common.Hash{}, db.originalRoot)
	prefetcher.close()
	cpy2 := cpy.copy()
	prefetchGuaranteed(cpy2, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	d := cpy2.trie(common.Hash{}, db.originalRoot)
	cpy.close()
	cpy2.close()
	if a.Hash() != b.Hash() || a.Hash() != c.Hash() || a.Hash() != d.Hash() {
		t.Fatalf("Invalid trie, hashes should be equal: %v %v %v %v", a.Hash(), b.Hash(), c.Hash(), d.Hash())
	}
}

func TestUseAfterClose(t *testing.T) {
	db := filledStateDB()
	prefetcher := newTriePrefetcher(db.db, db.originalRoot, common.Hash{}, "")
	skey := common.HexToHash("aaa")
	prefetchGuaranteed(prefetcher, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	a := prefetcher.trie(common.Hash{}, db.originalRoot)
	prefetcher.close()
	b := prefetcher.trie(common.Hash{}, db.originalRoot)
	if a == nil {
		t.Fatal("Prefetching before close should not return nil")
	}
	if b != nil {
		t.Fatal("Trie after close should return nil")
	}
}

func TestCopyClose(t *testing.T) {
	db := filledStateDB()
	prefetcher := newTriePrefetcher(db.db, db.originalRoot, common.Hash{}, "")
	skey := common.HexToHash("aaa")
	prefetchGuaranteed(prefetcher, common.Hash{}, db.originalRoot, common.Address{}, [][]byte{skey.Bytes()})
	cpy := prefetcher.copy()
	a := prefetcher.trie(common.Hash{}, db.originalRoot)
	b := cpy.trie(common.Hash{}, db.originalRoot)
	prefetcher.close()
	c := prefetcher.trie(common.Hash{}, db.originalRoot)
	d := cpy.trie(common.Hash{}, db.originalRoot)
	if a == nil {
		t.Fatal("Prefetching before close should not return nil")
	}
	if b == nil {
		t.Fatal("Copy trie should return nil")
	}
	if c != nil {
		t.Fatal("Trie after close should return nil")
	}
	if d == nil {
		t.Fatal("Copy trie should not return nil")
	}
}
