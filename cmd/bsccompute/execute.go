package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/trie"
)

type observation struct {
	duration time.Duration
	res      *core.ProcessResult
	statedb  *state.StateDB
}

type verified struct {
	charged    uint64
	commitment string
}

func (r *caseRun) execute(hooks *tracing.Hooks) (*observation, error) {
	statedb, err := r.bc.StateAt(r.parent)
	if err != nil {
		return nil, fmt.Errorf("restoring parent state: %w", err)
	}
	statedb.SetExpectedStateRoot(r.block.Root())
	statedb.SetNeedBadSharedStorage(r.bc.Config().NeedBadSharedStorage(r.block.Number()))
	statedb.StartPrefetcher("chain", nil)
	ctx := context.Background()
	cfg := vm.Config{Tracer: hooks}
	runtime.GC()

	start := time.Now()
	res, err := r.processor.Process(ctx, r.block, statedb, cfg)
	if err == nil {
		err = r.bc.Validator().ValidateState(r.block, statedb, res, false)
	}
	duration := time.Since(start)

	statedb.StopPrefetcher()
	if err != nil {
		return nil, err
	}
	return &observation{duration: duration, res: res, statedb: statedb}, nil
}

func (r *caseRun) verify(obs *observation) (*verified, error) {
	receipts := obs.res.Receipts
	users := len(r.wc.txs)
	if len(receipts) != users+1 {
		return nil, fmt.Errorf("execution produced %d receipts, want %d user + 1 deposit tail", len(receipts), users)
	}
	charged, system := splitGas(receipts, users)
	root := obs.statedb.IntermediateRoot(r.bc.Config().IsEIP158(r.block.Number()))
	receiptRoot := types.DeriveSha(receipts, trie.NewStackTrie(nil))
	bloom := types.MergeBloom(receipts)
	v := &verified{
		charged:    charged,
		commitment: digestHex(root[:], receiptRoot[:], bloom[:], u64(obs.res.GasUsed), u64(charged), u64(system), u64(uint64(len(receipts)))),
	}
	switch {
	case obs.res.GasUsed != charged+system:
		return v, fmt.Errorf("block gas %d is not user %d + system %d", obs.res.GasUsed, charged, system)
	case obs.res.GasUsed > r.block.GasLimit():
		return v, fmt.Errorf("block gas %d exceeds header gas limit %d", obs.res.GasUsed, r.block.GasLimit())
	case charged != r.chargedGas || system != r.systemGas:
		return v, fmt.Errorf("execution charged %d user + %d system gas, assembly charged %d + %d", charged, system, r.chargedGas, r.systemGas)
	}
	if err := checkValidatorDeposit(receipts[users], r.block.Coinbase()); err != nil {
		return v, err
	}
	for i, want := range r.wc.receipts {
		got := receipts[i]
		if (got.Status == types.ReceiptStatusSuccessful) != want.success {
			return v, fmt.Errorf("receipt %d status %d, expected success %t", i, got.Status, want.success)
		}
		if len(got.Logs) != len(want.logs) {
			return v, fmt.Errorf("receipt %d has %d logs, expected %d", i, len(got.Logs), len(want.logs))
		}
		for j, wl := range want.logs {
			gl := got.Logs[j]
			if gl.Address != wl.address || !slices.Equal(gl.Topics, wl.topics) || !bytes.Equal(gl.Data, wl.data) {
				return v, fmt.Errorf("receipt %d log %d differs from the independent expectation", i, j)
			}
		}
	}
	for _, slot := range r.wc.storage {
		if got := obs.statedb.GetState(slot.address, slot.slot); got != slot.value {
			return v, fmt.Errorf("storage %s:%s observed %s, expected %s", slot.address, slot.slot, got, slot.value)
		}
	}
	return v, nil
}

func (r *caseRun) importMeasured() error {
	if _, err := r.bc.InsertChain(types.Blocks{r.block}); err != nil {
		return fmt.Errorf("importing measured block: %w", err)
	}
	if head := r.bc.CurrentBlock(); head.Hash() != r.block.Hash() {
		return errors.New("measured block imported but is not the chain head")
	}
	return nil
}
