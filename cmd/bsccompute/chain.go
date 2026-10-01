package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/consensus/parlia"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/systemcontracts"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	parliaInitTxs  = 7
	feynmanInitTxs = 5
	sealTimeout    = 10 * time.Second
)

var (
	validatorContract      = common.HexToAddress(systemcontracts.ValidatorContract)
	depositSelector        = crypto.Keccak256([]byte("deposit(address)"))[:4]
	validatorDepositTopic  = crypto.Keccak256Hash([]byte("validatorDeposit(address,uint256)"))
	deprecatedDepositTopic = crypto.Keccak256Hash([]byte("deprecatedDeposit(address,uint256)"))
)

type caseRun struct {
	dev          *devnet
	wc           *workloadCase
	db           ethdb.Database
	bc           *core.BlockChain
	engine       *parlia.Parlia
	processor    *core.StateProcessor
	parent       *types.Header
	block        *types.Block
	senders      map[common.Address]struct{}
	declaredGas  uint64
	chargedGas   uint64
	systemGas    uint64
	baselineHash string
	preparedHash string
	imported     bool
}

type preparedFact struct {
	Event          string `json:"event"`
	CaseID         string `json:"case_id"`
	DeclaredGas    uint64 `json:"declared_gas"`
	UserGas        uint64 `json:"user_gas"`
	SystemGas      uint64 `json:"system_gas"`
	HeaderGasLimit uint64 `json:"header_gas_limit"`
	TotalGas       uint64 `json:"total_gas"`
	BlockNumber    uint64 `json:"block_number"`
	BlockHash      string `json:"block_hash"`
	BaselineHash   string `json:"baseline_hash"`
	PreparedHash   string `json:"prepared_hash"`
}

func newCaseRun(dev *devnet, wc *workloadCase, workloadSHA [32]byte) (*caseRun, string, error) {
	genesis, err := dev.caseGenesis(wc)
	if err != nil {
		return nil, stageBaselineSetup, err
	}
	r := &caseRun{dev: dev, wc: wc, db: rawdb.NewMemoryDatabase()}
	genesisHash := genesis.ToBlock().Hash()
	r.engine = parlia.New(dev.config, r.db, nil, genesisHash)
	dev.authorize(r.engine)
	if r.bc, err = core.NewBlockChain(r.db, genesis, r.engine, core.DefaultConfig()); err != nil {
		r.close()
		return nil, stageBaselineSetup, fmt.Errorf("creating blockchain: %w", err)
	}
	if err := r.checkIdentity(genesisHash); err != nil {
		r.close()
		return nil, stageBaselineSetup, err
	}
	r.processor = core.NewStateProcessor(r.bc.HeaderChain())
	if err := r.setup(); err != nil {
		r.close()
		return nil, stageBaselineSetup, err
	}
	if err := r.baseline(workloadSHA); err != nil {
		r.close()
		return nil, stageBaselineSetup, err
	}
	if err := r.prepare(); err != nil {
		r.close()
		return nil, stagePrepare, err
	}
	return r, "", nil
}

func (r *caseRun) close() {
	if r.bc != nil {
		r.bc.Stop()
	}
	r.db.Close()
}

func (r *caseRun) checkIdentity(genesisHash common.Hash) error {
	if got := r.bc.Genesis().Hash(); got != genesisHash {
		return fmt.Errorf("blockchain genesis %s differs from engine genesis %s", got, genesisHash)
	}
	chainCfg, err := json.Marshal(r.bc.Config())
	if err != nil {
		return err
	}
	engineCfg, err := json.Marshal(r.dev.config)
	if err != nil {
		return err
	}
	if !bytes.Equal(chainCfg, engineCfg) {
		return errors.New("blockchain chain config differs from the Parlia engine config")
	}
	return nil
}

func (r *caseRun) setup() error {
	parent := r.bc.CurrentBlock()
	for number := uint64(1); number < measuredNumber; number++ {
		var txs []*types.Transaction
		if number == measuredNumber-1 {
			tx, err := r.warmTransfer(parent)
			if err != nil {
				return fmt.Errorf("block %d warm transfer: %w", number, err)
			}
			txs = append(txs, tx)
		}
		block, receipts, fees, err := r.buildBlock(parent, txs)
		if err != nil {
			return fmt.Errorf("building setup block %d: %w", number, err)
		}
		if err := r.checkReceipts(block, parent, receipts, len(txs)); err != nil {
			return fmt.Errorf("setup block %d: %w", number, err)
		}
		for i, receipt := range receipts[:len(txs)] {
			if receipt.Status != types.ReceiptStatusSuccessful {
				return fmt.Errorf("setup block %d user transaction %d failed", number, i)
			}
		}
		if len(txs) > 0 {
			if err := checkDepositTail(block, receipts[len(receipts)-1], fees); err != nil {
				return fmt.Errorf("setup block %d: %w", number, err)
			}
		}
		if _, err := r.bc.InsertChain(types.Blocks{block}); err != nil {
			return fmt.Errorf("importing setup block %d: %w", number, err)
		}
		if head := r.bc.CurrentBlock(); head.Hash() != block.Hash() {
			return fmt.Errorf("setup block %d imported but head is %s", number, head.Hash())
		}
		parent = block.Header()
	}
	r.parent = parent
	return nil
}

func (r *caseRun) warmTransfer(parent *types.Header) (*types.Transaction, error) {
	statedb, err := r.bc.StateAt(parent)
	if err != nil {
		return nil, err
	}
	return types.SignNewTx(r.dev.validator, types.LatestSigner(r.dev.config), &types.DynamicFeeTx{
		ChainID:   r.dev.config.ChainID,
		Nonce:     statedb.GetNonce(r.dev.coinbase),
		GasTipCap: workloadTip,
		GasFeeCap: workloadTip,
		Gas:       params.TxGas,
		To:        &r.dev.coinbase,
		Value:     new(big.Int),
	})
}

func (r *caseRun) baseline(workloadSHA [32]byte) error {
	cfg, err := json.Marshal(r.bc.Config())
	if err != nil {
		return err
	}
	genesisHash := r.bc.Genesis().Hash()
	parentHash := r.parent.Hash()
	r.baselineHash = digestHex(r.dev.genesisSHA[:], genesisHash[:], parentHash[:], r.parent.Root[:], cfg, workloadSHA[:], []byte(r.wc.id))
	return nil
}

func (r *caseRun) buildBlock(parent *types.Header, txs []*types.Transaction) (*types.Block, []*types.Receipt, *big.Int, error) {
	cfg := r.dev.config
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     new(big.Int).Add(parent.Number, common.Big1),
		GasLimit:   core.CalcGasLimit(parent.GasLimit, headerGasLimit),
		Time:       parent.Time + 1,
	}
	if cfg.IsLondon(header.Number) {
		header.BaseFee = eip1559.CalcBaseFee(cfg, parent)
	}
	if err := r.engine.Prepare(r.bc, header); err != nil {
		return nil, nil, nil, fmt.Errorf("parlia prepare: %w", err)
	}
	if err := r.fixHistoricalTime(header, parent); err != nil {
		return nil, nil, nil, err
	}
	if err := r.guardOrdinary(header, parent); err != nil {
		return nil, nil, nil, err
	}
	if cfg.IsCancun(header.Number, header.Time) {
		var excessBlobGas uint64
		if cfg.IsCancun(parent.Number, parent.Time) {
			excessBlobGas = eip4844.CalcExcessBlobGas(cfg, parent, header.Time)
		}
		header.BlobGasUsed = new(uint64)
		header.ExcessBlobGas = &excessBlobGas
		header.WithdrawalsHash = &types.EmptyWithdrawalsHash
		if cfg.IsBohr(header.Number, header.Time) {
			header.ParentBeaconRoot = new(common.Hash)
		}
		if cfg.IsPrague(header.Number, header.Time) {
			header.RequestsHash = &types.EmptyRequestsHash
		}
	}
	statedb, err := r.bc.StateWithCacheAt(parent.Root)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parent state: %w", err)
	}
	reserved := min(r.engine.EstimateGasReservedForSystemTxs(r.bc, header), header.GasLimit)
	gp := core.NewGasPool(header.GasLimit - reserved)
	coinbase := header.Coinbase
	evm := vm.NewEVM(core.NewEVMBlockContext(header, r.bc, &coinbase), statedb, cfg, vm.Config{})
	systemcontracts.TryUpdateBuildInSystemContract(cfg, header.Number, parent.Time, header.Time, statedb, true)
	if header.ParentBeaconRoot != nil {
		core.ProcessBeaconBlockRoot(*header.ParentBeaconRoot, evm)
	}
	if cfg.IsPrague(header.Number, header.Time) {
		core.ProcessParentBlockHash(header.ParentHash, evm)
	}
	receipts := make([]*types.Receipt, 0, len(txs))
	for i, tx := range txs {
		statedb.SetTxContext(tx.Hash(), i)
		receipt, err := core.ApplyTransaction(evm, gp, statedb, header, tx)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("transaction %d (gas %d) rejected by production pool of %d (limit %d - reserve %d): %w",
				i, tx.Gas(), header.GasLimit-reserved, header.GasLimit, reserved, err)
		}
		receipts = append(receipts, receipt)
		header.GasUsed = gp.Used()
	}
	fees := statedb.GetBalance(consensus.SystemAddress).ToBig()
	body := types.Body{Transactions: txs}
	if header.EmptyWithdrawalsHash() {
		body.Withdrawals = make([]*types.Withdrawal, 0)
	}
	block, receipts, err := core.AssembleBlock(r.engine, r.bc, types.CopyHeader(header), statedb, &body, receipts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("assembling block: %w", err)
	}
	sealed, err := r.seal(block)
	if err != nil {
		return nil, nil, nil, err
	}
	if cfg.IsCancun(header.Number, header.Time) {
		sealed = sealed.WithSidecars(make(types.BlobSidecars, 0))
	}
	return sealed, receipts, fees, nil
}

func (r *caseRun) fixHistoricalTime(header, parent *types.Header) error {
	interval, err := r.engine.BlockInterval(r.bc, header)
	if err != nil {
		return fmt.Errorf("block interval: %w", err)
	}
	ms := parent.MilliTimestamp() + interval
	header.Time = ms / 1000
	if r.dev.config.IsLorentz(header.Number, header.Time) {
		header.SetMilliseconds(ms % 1000)
	} else {
		header.MixDigest = common.Hash{}
	}
	if err := r.engine.SetExtraData(r.bc, header); err != nil {
		return fmt.Errorf("parlia extra data: %w", err)
	}
	if now := uint64(time.Now().UnixMilli()); header.MilliTimestamp() >= now {
		return fmt.Errorf("block %d time %dms is not in the past", header.Number, header.MilliTimestamp())
	}
	return nil
}

func (r *caseRun) guardOrdinary(header, parent *types.Header) error {
	number := header.Number.Uint64()
	switch {
	case number >= 200:
		return fmt.Errorf("block %d reaches epoch or finality-reward heights served by the nil ethAPI", number)
	case header.Coinbase != r.dev.coinbase:
		return fmt.Errorf("block %d coinbase %s is not the devnet validator", number, header.Coinbase)
	case header.Difficulty == nil || header.Difficulty.Cmp(big.NewInt(2)) != 0:
		return fmt.Errorf("block %d is not in-turn (difficulty %v) and would enter the slash path", number, header.Difficulty)
	case parent.Time/params.BreatheBlockInterval != header.Time/params.BreatheBlockInterval:
		return fmt.Errorf("block %d crosses a UTC day and would be a breathe block", number)
	case r.dev.config.IsAmsterdam(header.Number, header.Time):
		return fmt.Errorf("block %d activates Amsterdam", number)
	}
	return nil
}

func (r *caseRun) seal(block *types.Block) (*types.Block, error) {
	results := make(chan *types.Block, 1)
	stop := make(chan struct{})
	defer close(stop)
	if err := r.engine.Seal(r.bc, block, results, stop); err != nil {
		return nil, fmt.Errorf("sealing block %d: %w", block.NumberU64(), err)
	}
	select {
	case sealed := <-results:
		return sealed, nil
	case <-time.After(sealTimeout):
		return nil, fmt.Errorf("sealing block %d produced no result within %s", block.NumberU64(), sealTimeout)
	}
}

func (r *caseRun) checkReceipts(block *types.Block, parent *types.Header, receipts []*types.Receipt, userTxs int) error {
	header := block.Header()
	system := 0
	if header.Number.Uint64() == 1 {
		system += parliaInitTxs
	}
	if r.dev.config.IsOnFeynman(header.Number, parent.Time, header.Time) {
		system += feynmanInitTxs
	}
	if userTxs > 0 {
		system++
	}
	if len(block.Transactions()) != userTxs+system || len(receipts) != userTxs+system {
		return fmt.Errorf("block %d carries %d transactions and %d receipts, want %d user + %d system",
			header.Number, len(block.Transactions()), len(receipts), userTxs, system)
	}
	for i, receipt := range receipts[userTxs:] {
		if receipt.Status != types.ReceiptStatusSuccessful {
			return fmt.Errorf("block %d system transaction %d failed", header.Number, i)
		}
	}
	return nil
}

func checkDepositTail(block *types.Block, receipt *types.Receipt, fees *big.Int) error {
	txs := block.Transactions()
	tail := txs[len(txs)-1]
	want := append(bytes.Clone(depositSelector), common.LeftPadBytes(block.Coinbase().Bytes(), common.HashLength)...)
	switch {
	case tail.To() == nil || *tail.To() != validatorContract:
		return fmt.Errorf("block %d tail is not addressed to ValidatorSet", block.NumberU64())
	case !bytes.Equal(tail.Data(), want):
		return fmt.Errorf("block %d tail is not deposit(%s)", block.NumberU64(), block.Coinbase())
	case fees.Sign() <= 0 || tail.Value().Cmp(fees) != 0:
		return fmt.Errorf("block %d tail deposits %s, block fees are %s", block.NumberU64(), tail.Value(), fees)
	}
	return checkValidatorDeposit(receipt, block.Coinbase())
}

func checkValidatorDeposit(receipt *types.Receipt, coinbase common.Address) error {
	found := false
	for _, l := range receipt.Logs {
		if l.Address != validatorContract || len(l.Topics) == 0 {
			continue
		}
		switch l.Topics[0] {
		case deprecatedDepositTopic:
			return errors.New("ValidatorSet emitted deprecatedDeposit for the coinbase")
		case validatorDepositTopic:
			if len(l.Topics) == 2 && l.Topics[1] == common.BytesToHash(coinbase.Bytes()) {
				found = true
			}
		}
	}
	if !found {
		return errors.New("deposit tail emitted no validatorDeposit for the coinbase")
	}
	return nil
}

func checkMeasuredRules(cfg *params.ChainConfig, header *types.Header) error {
	if !cfg.IsOsaka(header.Number, header.Time) || !cfg.IsPasteur(header.Number, header.Time) || cfg.IsAmsterdam(header.Number, header.Time) {
		return fmt.Errorf("block %d is not Osaka+Pasteur without Amsterdam", header.Number)
	}
	active := vm.ActivePrecompiledContracts(cfg.Rules(header.Number, false, header.Time))
	if len(active) != len(vm.PrecompiledContractsPasteur) {
		return fmt.Errorf("block %d activates %d precompiles, Pasteur defines %d", header.Number, len(active), len(vm.PrecompiledContractsPasteur))
	}
	for addr, want := range vm.PrecompiledContractsPasteur {
		if got, ok := active[addr]; !ok || reflect.TypeOf(got) != reflect.TypeOf(want) {
			return fmt.Errorf("block %d precompile %s is not the Pasteur implementation", header.Number, addr)
		}
	}
	return nil
}

func (r *caseRun) prepare() error {
	cfg := r.dev.config
	signer := types.LatestSigner(cfg)
	nonces := make(map[common.Address]uint64, len(r.wc.txs))
	r.senders = make(map[common.Address]struct{}, len(r.wc.txs))
	txs := make([]*types.Transaction, 0, len(r.wc.txs))
	for i, wt := range r.wc.txs {
		if wt.sender == r.dev.coinbase {
			return fmt.Errorf("transaction %d is sent by the devnet validator", i)
		}
		nonce, seen := nonces[wt.sender]
		if !seen {
			if pa, ok := r.wc.pre[wt.sender]; ok {
				nonce = pa.nonce
			}
		}
		nonces[wt.sender] = nonce + 1
		r.senders[wt.sender] = struct{}{}
		r.declaredGas += wt.gasLimit
		to := wt.to
		tx, err := types.SignNewTx(wt.key, signer, &types.DynamicFeeTx{
			ChainID:   cfg.ChainID,
			Nonce:     nonce,
			GasTipCap: workloadTip,
			GasFeeCap: workloadTip,
			Gas:       wt.gasLimit,
			To:        &to,
			Value:     wt.value,
			Data:      wt.data,
		})
		if err != nil {
			return fmt.Errorf("signing transaction %d: %w", i, err)
		}
		txs = append(txs, tx)
	}
	block, receipts, fees, err := r.buildBlock(r.parent, txs)
	if err != nil {
		return err
	}
	header := block.Header()
	if header.Number.Uint64() != measuredNumber || header.GasLimit != headerGasLimit {
		return fmt.Errorf("measured block is %d with gas limit %d, want %d with %d", header.Number, header.GasLimit, measuredNumber, headerGasLimit)
	}
	if reserve := r.engine.EstimateGasReservedForSystemTxs(r.bc, header); reserve != params.SystemTxsGasSoftLimit {
		return fmt.Errorf("measured block reserves %d gas for system transactions, want %d", reserve, params.SystemTxsGasSoftLimit)
	}
	if err := checkMeasuredRules(cfg, header); err != nil {
		return err
	}
	if err := r.checkReceipts(block, r.parent, receipts, len(txs)); err != nil {
		return err
	}
	if err := checkDepositTail(block, receipts[len(receipts)-1], fees); err != nil {
		return err
	}
	r.chargedGas, r.systemGas = splitGas(receipts, len(txs))
	if header.GasUsed != r.chargedGas+r.systemGas || header.GasUsed > header.GasLimit {
		return fmt.Errorf("measured header gas %d is not user %d + system %d within limit %d", header.GasUsed, r.chargedGas, r.systemGas, header.GasLimit)
	}
	if err := r.engine.VerifyHeader(r.bc, header); err != nil {
		return fmt.Errorf("verifying measured header: %w", err)
	}
	if err := r.bc.Validator().ValidateBody(block); err != nil {
		return fmt.Errorf("validating measured body: %w", err)
	}
	blockSigner := types.MakeSigner(cfg, header.Number, header.Time)
	if !blockSigner.Equal(signer) {
		return errors.New("measured block signer differs from the latest signer used by Parlia")
	}
	for i, tx := range block.Transactions() {
		want := r.dev.coinbase
		if i < len(r.wc.txs) {
			want = r.wc.txs[i].sender
		}
		if from, err := types.Sender(blockSigner, tx); err != nil || from != want {
			return fmt.Errorf("transaction %d recovers %s (%v), want %s", i, from, err, want)
		}
	}
	encoded, err := rlp.EncodeToBytes(block)
	if err != nil {
		return fmt.Errorf("encoding measured block: %w", err)
	}
	r.block = block
	r.preparedHash = digestHex([]byte(r.baselineHash), encoded)
	return json.NewEncoder(os.Stderr).Encode(preparedFact{
		Event:          "bsc_case_prepared",
		CaseID:         r.wc.id,
		DeclaredGas:    r.declaredGas,
		UserGas:        r.chargedGas,
		SystemGas:      r.systemGas,
		HeaderGasLimit: header.GasLimit,
		TotalGas:       header.GasUsed,
		BlockNumber:    header.Number.Uint64(),
		BlockHash:      block.Hash().Hex(),
		BaselineHash:   r.baselineHash,
		PreparedHash:   r.preparedHash,
	})
}

func splitGas(receipts []*types.Receipt, userTxs int) (charged, system uint64) {
	if userTxs > 0 {
		charged = receipts[userTxs-1].CumulativeGasUsed
	}
	for _, receipt := range receipts[userTxs:] {
		system += receipt.GasUsed
	}
	return charged, system
}

func u64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}
