package main

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

type opCounter struct {
	senders         map[common.Address]struct{}
	watched         *common.Address
	inUserTx        bool
	inSystem        bool
	opcodes         [256]uint64
	precompileCalls uint64
}

func newOpCounter(senders map[common.Address]struct{}, watched *common.Address) *opCounter {
	return &opCounter{senders: senders, watched: watched}
}

func (c *opCounter) counting() bool {
	return c.inUserTx && !c.inSystem
}

func (c *opCounter) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnTxStart: func(_ *tracing.VMContext, _ *types.Transaction, from common.Address) {
			_, workload := c.senders[from]
			c.inUserTx = workload && !c.inSystem
		},
		OnTxEnd:             func(*types.Receipt, error) { c.inUserTx = false },
		OnSystemCallStartV2: func(*tracing.VMContext) { c.inSystem = true },
		OnSystemCallEnd:     func() { c.inSystem = false },
		OnSystemTxStart:     func() { c.inSystem = true },
		OnSystemTxEnd:       func() { c.inSystem = false },
		OnOpcode: func(_ uint64, op byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
			if c.counting() {
				c.opcodes[op]++
			}
		},
		OnEnter: func(_ int, _ byte, _ common.Address, to common.Address, _ []byte, _ uint64, _ *big.Int) {
			if c.counting() && c.watched != nil && to == *c.watched {
				c.precompileCalls++
			}
		},
	}
}

func (c *opCounter) counts() map[string]uint64 {
	out := make(map[string]uint64)
	for op, n := range c.opcodes {
		if n > 0 {
			out[vm.OpCode(op).String()] = n
		}
	}
	if c.watched != nil {
		out[precompileKey(*c.watched)] = c.precompileCalls
	}
	return out
}

func (c *opCounter) target(semanticKey string) (uint64, error) {
	if c.watched != nil && semanticKey == precompileKey(*c.watched) {
		return c.precompileCalls, nil
	}
	if !knownOpcode(semanticKey) {
		return 0, fmt.Errorf("target %s is not a recognized opcode or watched precompile", semanticKey)
	}
	return c.opcodes[vm.StringToOp(semanticKey)], nil
}
