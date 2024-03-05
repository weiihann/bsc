package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const (
	batchKeySize   = 100000
	batchBlockSize = 50000
)

type keyToBlockNumMsg struct {
	addr     common.Hash // hash(addr)
	key      common.Hash // hash(key)
	blockNum uint64
}

type KeyValueAnalyser struct {
	db ethdb.Database

	mainMsgChan chan *keyToBlockNumMsg
	flushChan   chan struct{}
	closeChan   chan struct{}

	accountMu sync.RWMutex
	storageMu sync.RWMutex
	accounts  map[common.Hash]uint64
	storages  map[common.Hash]map[common.Hash]uint64

	blockCount uint64
}

func NewKeyValueAnalyser(db ethdb.Database) *KeyValueAnalyser {
	ka := &KeyValueAnalyser{
		db:          db,
		mainMsgChan: make(chan *keyToBlockNumMsg),
		flushChan:   make(chan struct{}),
		closeChan:   make(chan struct{}),
		accounts:    make(map[common.Hash]uint64),
		storages:    make(map[common.Hash]map[common.Hash]uint64),
		blockCount:  0,
	}

	go ka.Start()
	return ka
}

func (ka *KeyValueAnalyser) Start() {
	for {
		select {
		case msg := <-ka.mainMsgChan:
			ka.addKey(msg)
		case <-ka.flushChan:
			log.Info("Flushing KeyValueAnalyser data to db")
			err := ka.writeToDB()
			if err != nil {
				log.Error("Failed to flush KeyValueAnalyser data to db", "err", err)
				return
			}
		case <-ka.closeChan:
			log.Info("Closing KeyValueAnalyser, flushing remaining data to db")
			err := ka.writeToDB()
			close(ka.flushChan)
			close(ka.mainMsgChan)
			if err != nil {
				log.Error("Failed to flush KeyValueAnalyser data to db", "err", err)
			}
			return
		}
	}
}

func (ka *KeyValueAnalyser) addKey(msg *keyToBlockNumMsg) {
	if msg.key == (common.Hash{}) { // access account
		ka.accountMu.Lock()
		defer ka.accountMu.Unlock()
		if prev, exist := ka.accounts[msg.addr]; !exist || prev < msg.blockNum {
			ka.accounts[msg.addr] = msg.blockNum
		}
	} else { // access storage
		ka.storageMu.Lock()
		defer ka.storageMu.Unlock()
		if _, exist := ka.storages[msg.addr]; !exist {
			ka.storages[msg.addr] = make(map[common.Hash]uint64)
		}

		if prev, exist := ka.storages[msg.addr][msg.key]; !exist || prev < msg.blockNum {
			ka.storages[msg.addr][msg.key] = msg.blockNum
		}
	}

	if len(ka.accounts)+len(ka.storages) >= batchKeySize {
		ka.flushChan <- struct{}{}
	}
}

// Flush the accounts and storages to the database, and clear the maps
func (ka *KeyValueAnalyser) writeToDB() error {
	batch := ka.db.NewBatch()

	// Write account snapshot meta
	ka.accountMu.Lock()
	for addr, blockNum := range ka.accounts {
		rawdb.WriteAccountSnapshotMeta(batch, addr, blockNum)
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				log.Error("Failed to write account snapshot meta", "err", err)
				ka.accountMu.Unlock()
				return err
			}
			batch.Reset()
		}
	}
	ka.accounts = make(map[common.Hash]uint64)
	ka.accountMu.Unlock()

	// Write storage snapshot meta
	ka.storageMu.Lock()
	for addr, storage := range ka.storages {
		for key, blockNum := range storage {
			rawdb.WriteStorageSnapshotMeta(batch, addr, key, blockNum)
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				log.Error("Failed to write storage snapshot meta", "err", err)
				ka.storageMu.Unlock()
				return err
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil { // Write remaining data
		log.Error("Failed to write remaining snapshot meta", "err", err)
		ka.storageMu.Unlock()
		return err
	}
	batch.Reset()

	ka.storages = make(map[common.Hash]map[common.Hash]uint64)
	ka.storageMu.Unlock()

	return nil
}

func (ka *KeyValueAnalyser) Close() {
	close(ka.closeChan)
}

func (ka *KeyValueAnalyser) GetAccountCount() int {
	ka.accountMu.RLock()
	defer ka.accountMu.RUnlock()
	return len(ka.accounts)
}

func (ka *KeyValueAnalyser) GetStorageCount() int {
	ka.storageMu.RLock()
	defer ka.storageMu.RUnlock()
	return len(ka.storages)
}

func (ka *KeyValueAnalyser) UpdateBlockCount() {
	ka.blockCount++
	if ka.blockCount >= batchBlockSize {
		ka.flushChan <- struct{}{}
		ka.blockCount = 0
	}
}
