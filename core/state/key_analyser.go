package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const (
	batchSize = 1000000
)

type keyToBlockNumMsg struct {
	addr     common.Hash // hash(addr)
	key      common.Hash // hash(key)
	blockNum uint64
}

type keyValueAnalyser struct {
	db Database

	mainMsgChan chan *keyToBlockNumMsg
	flushChan   chan struct{}
	closeChan   chan struct{}

	accountMu sync.RWMutex
	storageMu sync.RWMutex
	accounts  map[common.Hash]uint64
	storages  map[common.Hash]map[common.Hash]uint64
}

func NewKeyValueAnalyser(db Database) *keyValueAnalyser {
	ka := &keyValueAnalyser{
		db:          db,
		mainMsgChan: make(chan *keyToBlockNumMsg),
		flushChan:   make(chan struct{}),
		closeChan:   make(chan struct{}),
		accounts:    make(map[common.Hash]uint64),
		storages:    make(map[common.Hash]map[common.Hash]uint64),
	}

	go ka.Start()
	return ka
}

func (ka *keyValueAnalyser) Start() {
	for {
		select {
		case msg := <-ka.mainMsgChan:
			ka.addKey(msg)
		case <-ka.flushChan:
			err := ka.writeToDB()
			if err != nil {
				return
			}
		case <-ka.closeChan:
			log.Info("Closing keyValueAnalyser, flushing remaining data to db")
			ka.writeToDB()
			return
		}
	}
}

func (ka *keyValueAnalyser) addKey(msg *keyToBlockNumMsg) {
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

	if len(ka.accounts)+len(ka.storages) >= batchSize {
		ka.flushChan <- struct{}{}
	}
}

// Flush the accounts and storages to the database, and clear the maps
func (ka *keyValueAnalyser) writeToDB() error {
	batch := ka.db.DiskDB().NewBatch()

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
	if err := batch.Write(); err != nil { // Write remaining data
		log.Error("Failed to write account snapshot meta", "err", err)
		ka.accountMu.Unlock()
		return err
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
		log.Error("Failed to write storage snapshot meta", "err", err)
		ka.storageMu.Unlock()
		return err
	}
	ka.storages = make(map[common.Hash]map[common.Hash]uint64)
	ka.storageMu.Unlock()

	return nil
}

func (ka *keyValueAnalyser) Close() {
	close(ka.mainMsgChan)
	close(ka.flushChan)
	close(ka.closeChan)
}
