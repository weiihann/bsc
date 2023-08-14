package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"

	"github.com/olekukonko/tablewriter"
)

type MetaInspector struct {
	db          ethdb.Database
	fromBlock   uint64
	toBlock     uint64
	bucketRange uint64

	resultAcc   *[]*stat
	resultStore *[]*stat
	totalAcc    stat
	totalStore  stat
}

func NewMetaInspector(db ethdb.Database, fromBlock, toBlock, bucketRange uint64) (*MetaInspector, error) {
	if db == nil {
		return nil, fmt.Errorf("nil db")
	}

	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range")
	}

	// Create a list of buckets for the given range
	bucketNum := (toBlock-fromBlock+1)/bucketRange + 1
	log.Info("Creating buckets", "bucketNum", bucketNum)
	bucketsAcc := make([]*stat, bucketNum)
	bucketsStore := make([]*stat, bucketNum)

	for i := uint64(0); i < bucketNum; i++ {
		bucketsAcc[i] = &stat{}
		bucketsStore[i] = &stat{}
	}

	ins := &MetaInspector{
		db:          db,
		fromBlock:   fromBlock,
		toBlock:     toBlock,
		resultAcc:   &bucketsAcc,
		resultStore: &bucketsStore,
		bucketRange: bucketRange,
	}

	return ins, nil
}

func (ins *MetaInspector) Run() {
	err := ins.IterateAccountMeta()
	if err != nil {
		panic(err)
	}

	err = ins.IterateStorageMeta()
	if err != nil {
		panic(err)
	}
}

func (ins *MetaInspector) IterateAccountMeta() error {
	log.Info("Inspecting account meta")
	db := ins.db
	it := db.NewIterator(SnapshotAccountMetaPrefix, []byte{})
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()
	)

	for it.Next() {

		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}

		key := it.Key()
		val := it.Value()

		if !bytes.HasPrefix(key, SnapshotAccountMetaPrefix) || !(len(key) == (len(SnapshotAccountMetaPrefix) + common.HashLength)) {
			continue
		}

		ins.totalAcc.Add(1) // ignore size

		valBlockNum := binary.BigEndian.Uint64(val)
		if !ins.include(valBlockNum) {
			continue
		}

		bucketNum := ins.resolveBucket(valBlockNum)
		bucket := (*ins.resultAcc)[bucketNum]
		bucket.Add(1) // ignore size
	}

	return nil
}

func (ins *MetaInspector) IterateStorageMeta() error {
	log.Info("Inspecting storage meta")
	db := ins.db
	it := db.NewIterator(SnapshotStorageMetaPrefix, []byte{})
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()
	)

	for it.Next() {

		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}

		key := it.Key()
		val := it.Value()

		if !bytes.HasPrefix(key, SnapshotStorageMetaPrefix) || !(len(key) == (len(SnapshotStorageMetaPrefix) + 2*common.HashLength)) {
			continue
		}

		ins.totalStore.Add(1) // ignore size

		valBlockNum := binary.BigEndian.Uint64(val)
		if !ins.include(valBlockNum) {
			continue
		}

		bucketNum := ins.resolveBucket(valBlockNum)
		bucket := (*ins.resultStore)[bucketNum]
		bucket.Add(1) // ignore size
	}

	return nil
}

func (ins *MetaInspector) include(blockNum uint64) bool {
	return blockNum >= ins.fromBlock && blockNum <= ins.toBlock
}

func (ins *MetaInspector) resolveBucket(blockNum uint64) uint64 {
	return blockNum / ins.bucketRange
}

func (ins *MetaInspector) Display() {
	fmt.Printf("Number of key-value pairs accessed from block %d to block %d\n", ins.fromBlock, ins.toBlock)
	table := tablewriter.NewWriter(os.Stdout)
	resultLen := len(*ins.resultAcc)

	stats := make([][]string, resultLen)
	total := ins.totalAcc.count + ins.totalStore.count

	rangeAccCount := counter(0)
	rangeStoreCount := counter(0)

	for i := 0; i < resultLen; i++ {
		bucketAcc := (*ins.resultAcc)[i]
		bucketStore := (*ins.resultStore)[i]
		rangeAccCount += bucketAcc.count
		rangeStoreCount += bucketStore.count
		totalBucket := bucketAcc.count + bucketStore.count

		low := uint64(i) * ins.bucketRange
		high := uint64(i+1)*ins.bucketRange - 1

		if i == 0 {
			low = ins.fromBlock
		}

		if high > ins.toBlock {
			high = ins.toBlock
		}

		rowStr := fmt.Sprintf("%d-%d", low, high)

		bucketAccPerc := calcPercentage(bucketAcc.count, ins.totalAcc.count)
		bucketStorePerc := calcPercentage(bucketStore.count, ins.totalStore.count)
		totalBucketPerc := calcPercentage(totalBucket, total)

		stats = append(stats, []string{rowStr, bucketAcc.Count(), bucketStore.Count(), bucketAccPerc, bucketStorePerc, totalBucketPerc})
	}

	rangeAccPerc := calcPercentage(rangeAccCount, ins.totalAcc.count)
	rangeStorePerc := calcPercentage(rangeStoreCount, ins.totalStore.count)
	totalRangePerc := calcPercentage(rangeAccCount+rangeStoreCount, total)

	totalAccPerc := calcPercentage(ins.totalAcc.count, total)
	totalStorePerc := calcPercentage(ins.totalStore.count, total)

	table.SetAlignment(1)
	table.SetHeader([]string{"BLOCK RANGE", "ACCOUNT COUNT", "STORAGE COUNT", "ACCOUNT PERCENTAGE", "STORAGE PERCENTAGE", "RANGE PERCENTAGE"})
	table.SetFooter([]string{"Total", rangeAccCount.String(), rangeStoreCount.String(), rangeAccPerc, rangeStorePerc, totalRangePerc})
	table.AppendBulk(stats)
	table.Render()

	fmt.Printf("Total account KV count from genesis block to the latest block: %s (%s)\n", ins.totalAcc.count.String(), totalAccPerc)
	fmt.Printf("Total storage KV count from genesis block to the latest block: %s (%s)\n", ins.totalStore.count.String(), totalStorePerc)
	fmt.Printf("Total KV accessed from block %d to block %d: %s (%s)\n", ins.fromBlock, ins.toBlock, rangeAccCount+rangeStoreCount, totalRangePerc)
}

func calcPercentage(a, b counter) string {
	return fmt.Sprintf("%.2f%%", float64(a)/float64(b)*100)
}
