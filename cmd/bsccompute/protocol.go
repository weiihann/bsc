package main

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

const (
	schemaVersion     = 2
	supportedFork     = "Osaka"
	executionBoundary = "bsc_block_execution"

	modeDiagnostic  = "diagnostic"
	modePerformance = "performance"

	phaseDiagnostic    = "diagnostic"
	phasePilot         = "pilot"
	phaseWarmup        = "warmup"
	phaseQualification = "qualification"

	statusExecuted    = "executed"
	statusFailed      = "failed"
	statusUnsupported = "unsupported"

	caseStatusReady       = "ready"
	caseStatusUnsupported = "unsupported"

	precompileKeyPrefix = "PRECOMPILE_"
)

const (
	stageLoadRequest   = "load_request"
	stageLoadWorkload  = "load_workload"
	stageBaselineSetup = "baseline_setup"
	stagePrepare       = "prepare"
	stageExecute       = "execute"
	stageVerify        = "verify"
	stageDiagnostic    = "diagnostic"
	stageImport        = "import"
)

type request struct {
	SchemaVersion int             `json:"schema_version"`
	WorkloadPath  string          `json:"workload_path"`
	SessionID     string          `json:"session_id"`
	Mode          string          `json:"mode"`
	Samples       []requestSample `json:"samples"`
}

type requestSample struct {
	SampleID   string `json:"sample_id"`
	CaseID     string `json:"case_id"`
	Repetition uint64 `json:"repetition"`
	Phase      string `json:"phase"`
}

type result struct {
	SchemaVersion       int               `json:"schema_version"`
	SessionID           string            `json:"session_id"`
	SampleID            string            `json:"sample_id"`
	CaseID              string            `json:"case_id"`
	Repetition          uint64            `json:"repetition"`
	Phase               string            `json:"phase"`
	Status              string            `json:"status"`
	ExecutionDurationNS *uint64           `json:"execution_duration_ns"`
	ExecutionBoundary   string            `json:"execution_boundary"`
	BaselineHash        *string           `json:"baseline_hash"`
	PreparedHash        *string           `json:"prepared_hash"`
	CommitmentHash      *string           `json:"commitment_hash"`
	CorrectnessPassed   bool              `json:"correctness_passed"`
	TargetCount         *uint64           `json:"target_count"`
	OpcodeCounts        map[string]uint64 `json:"opcode_counts"`
	DeclaredGas         *uint64           `json:"declared_gas"`
	ChargedGas          *uint64           `json:"charged_gas"`
	Error               *resultError      `json:"error,omitempty"`
}

type resultError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

type workloadFile struct {
	SchemaVersion int    `json:"schema_version"`
	Fork          string `json:"fork"`
	Generator     struct {
		Revision string `json:"revision"`
	} `json:"generator"`
	Cases []rawCase `json:"cases"`
}

type rawCase struct {
	ID              string                     `json:"id"`
	Status          string                     `json:"status"`
	Reason          string                     `json:"reason"`
	Family          string                     `json:"family"`
	TargetOperation string                     `json:"target_operation"`
	Parameters      map[string]json.RawMessage `json:"parameters"`
	Pre             map[string]rawAccount      `json:"pre"`
	Transactions    []rawTransaction           `json:"transactions"`
	Expected        *rawExpected               `json:"expected"`
}

type rawAccount struct {
	Balance string            `json:"balance"`
	Nonce   string            `json:"nonce"`
	Code    string            `json:"code"`
	Storage map[string]string `json:"storage"`
}

type rawTransaction struct {
	Sender    string `json:"sender"`
	SecretKey string `json:"secret_key"`
	To        string `json:"to"`
	Data      string `json:"data"`
	Value     string `json:"value"`
	GasLimit  uint64 `json:"gas_limit"`
}

type rawExpected struct {
	Receipts []struct {
		Success *bool `json:"success"`
		Logs    []struct {
			Address string   `json:"address"`
			Topics  []string `json:"topics"`
			Data    string   `json:"data"`
		} `json:"logs"`
	} `json:"receipts"`
	Storage map[string]map[string]string `json:"storage"`
}

type preAccount struct {
	balance *big.Int
	nonce   uint64
	code    []byte
	storage map[common.Hash]common.Hash
}

type workloadTx struct {
	sender   common.Address
	key      *ecdsa.PrivateKey
	to       common.Address
	data     []byte
	value    *big.Int
	gasLimit uint64
}

type expectedLog struct {
	address common.Address
	topics  []common.Hash
	data    []byte
}

type expectedReceipt struct {
	success bool
	logs    []expectedLog
}

type expectedSlot struct {
	address common.Address
	slot    common.Hash
	value   common.Hash
}

type workloadCase struct {
	id                  string
	semanticKey         string
	watched             *common.Address
	expectedTargetCount *uint64
	pre                 map[common.Address]preAccount
	txs                 []workloadTx
	receipts            []expectedReceipt
	storage             []expectedSlot
}

type workload struct {
	sha         [32]byte
	ready       map[string]rawCase
	unsupported map[string]string
}

func loadWorkload(raw []byte) (*workload, error) {
	var file workloadFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("decoding workload: %w", err)
	}
	if file.SchemaVersion != schemaVersion {
		return nil, fmt.Errorf("workload schema_version %d is not supported (want %d)", file.SchemaVersion, schemaVersion)
	}
	if file.Fork != supportedFork {
		return nil, fmt.Errorf("workload fork %q is not supported (want %q)", file.Fork, supportedFork)
	}
	if strings.TrimSpace(file.Generator.Revision) == "" {
		return nil, errors.New("workload generator.revision is required")
	}
	if len(file.Cases) == 0 {
		return nil, errors.New("workload contains no cases")
	}
	w := &workload{
		sha:         sha256.Sum256(raw),
		ready:       make(map[string]rawCase, len(file.Cases)),
		unsupported: make(map[string]string),
	}
	for _, c := range file.Cases {
		if strings.TrimSpace(c.ID) == "" {
			return nil, errors.New("workload contains a case with an empty id")
		}
		if _, dup := w.ready[c.ID]; dup {
			return nil, fmt.Errorf("duplicate workload case id %q", c.ID)
		}
		if _, dup := w.unsupported[c.ID]; dup {
			return nil, fmt.Errorf("duplicate workload case id %q", c.ID)
		}
		switch c.Status {
		case caseStatusReady:
			w.ready[c.ID] = c
		case caseStatusUnsupported:
			if strings.TrimSpace(c.Reason) == "" {
				return nil, fmt.Errorf("unsupported case %q carries no reason", c.ID)
			}
			w.unsupported[c.ID] = c.Reason
		default:
			return nil, fmt.Errorf("case %q has unknown status %q", c.ID, c.Status)
		}
	}
	return w, nil
}

func parseCase(c rawCase) (*workloadCase, error) {
	if strings.TrimSpace(c.Family) == "" {
		return nil, errors.New("ready case requires a family")
	}
	if strings.TrimSpace(c.TargetOperation) == "" {
		return nil, errors.New("ready case requires target_operation")
	}
	if len(c.Parameters) == 0 {
		return nil, errors.New("ready case requires a parameters object")
	}
	if len(c.Transactions) == 0 {
		return nil, errors.New("ready case requires transactions")
	}
	if c.Expected == nil {
		return nil, errors.New("ready case requires expected outcomes")
	}
	if len(c.Expected.Receipts) != len(c.Transactions) {
		return nil, fmt.Errorf("expected %d receipts for %d transactions", len(c.Expected.Receipts), len(c.Transactions))
	}
	wc := &workloadCase{id: c.ID, pre: make(map[common.Address]preAccount, len(c.Pre))}
	if err := wc.parseTarget(c.TargetOperation, c.Parameters); err != nil {
		return nil, err
	}
	for rawAddr, acct := range c.Pre {
		addr, err := parseAddress(rawAddr)
		if err != nil {
			return nil, fmt.Errorf("pre address: %w", err)
		}
		if _, dup := wc.pre[addr]; dup {
			return nil, fmt.Errorf("pre address %s appears twice", addr)
		}
		pa, err := parsePreAccount(acct)
		if err != nil {
			return nil, fmt.Errorf("pre account %s: %w", addr, err)
		}
		wc.pre[addr] = pa
	}
	for i, tx := range c.Transactions {
		parsed, err := parseTransaction(tx)
		if err != nil {
			return nil, fmt.Errorf("transaction %d: %w", i, err)
		}
		wc.txs = append(wc.txs, parsed)
	}
	for i, r := range c.Expected.Receipts {
		if r.Success == nil {
			return nil, fmt.Errorf("expected receipt %d must state success", i)
		}
		er := expectedReceipt{success: *r.Success}
		for j, l := range r.Logs {
			addr, err := parseAddress(l.Address)
			if err != nil {
				return nil, fmt.Errorf("expected receipt %d log %d address: %w", i, j, err)
			}
			data, err := parseBytes(l.Data)
			if err != nil {
				return nil, fmt.Errorf("expected receipt %d log %d data: %w", i, j, err)
			}
			el := expectedLog{address: addr, data: data}
			for k, t := range l.Topics {
				topic, err := parseWord(t)
				if err != nil {
					return nil, fmt.Errorf("expected receipt %d log %d topic %d: %w", i, j, k, err)
				}
				el.topics = append(el.topics, topic)
			}
			er.logs = append(er.logs, el)
		}
		wc.receipts = append(wc.receipts, er)
	}
	for rawAddr, slots := range c.Expected.Storage {
		addr, err := parseAddress(rawAddr)
		if err != nil {
			return nil, fmt.Errorf("expected storage address: %w", err)
		}
		for rawSlot, rawValue := range slots {
			slot, err := parseWord(rawSlot)
			if err != nil {
				return nil, fmt.Errorf("expected storage slot of %s: %w", addr, err)
			}
			value, err := parseWord(rawValue)
			if err != nil {
				return nil, fmt.Errorf("expected storage value of %s slot %s: %w", addr, slot, err)
			}
			wc.storage = append(wc.storage, expectedSlot{address: addr, slot: slot, value: value})
		}
	}
	return wc, nil
}

func (wc *workloadCase) parseTarget(targetOperation string, parameters map[string]json.RawMessage) error {
	if raw, ok := parameters["precompile_address"]; ok {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return fmt.Errorf("parameters.precompile_address: %w", err)
		}
		addr, err := parseAddress(text)
		if err != nil {
			return fmt.Errorf("parameters.precompile_address: %w", err)
		}
		wc.watched = &addr
		wc.semanticKey = precompileKey(addr)
		if raw, ok := parameters["requested_opcode_count"]; ok {
			var count uint64
			if err := json.Unmarshal(raw, &count); err != nil {
				return fmt.Errorf("parameters.requested_opcode_count must be an unsigned integer: %w", err)
			}
			wc.expectedTargetCount = &count
		}
		return nil
	}
	op := strings.ToUpper(strings.TrimSpace(targetOperation))
	if op == "SHA3" {
		return errors.New("target_operation must use KECCAK256, not SHA3")
	}
	wc.semanticKey = op
	return nil
}

func precompileKey(addr common.Address) string {
	return precompileKeyPrefix + "0x" + hex.EncodeToString(addr.Bytes())
}

func knownOpcode(name string) bool {
	return vm.StringToOp(name).String() == name
}

func parsePreAccount(acct rawAccount) (preAccount, error) {
	balance, err := parseQuantity(acct.Balance)
	if err != nil {
		return preAccount{}, fmt.Errorf("balance: %w", err)
	}
	nonce, err := parseQuantity(acct.Nonce)
	if err != nil {
		return preAccount{}, fmt.Errorf("nonce: %w", err)
	}
	if !nonce.IsUint64() {
		return preAccount{}, fmt.Errorf("nonce %s overflows uint64", acct.Nonce)
	}
	pa := preAccount{balance: balance, nonce: nonce.Uint64(), storage: make(map[common.Hash]common.Hash, len(acct.Storage))}
	if acct.Code != "" {
		if pa.code, err = parseBytes(acct.Code); err != nil {
			return preAccount{}, fmt.Errorf("code: %w", err)
		}
	}
	for rawSlot, rawValue := range acct.Storage {
		slot, err := parseWord(rawSlot)
		if err != nil {
			return preAccount{}, fmt.Errorf("storage slot: %w", err)
		}
		value, err := parseWord(rawValue)
		if err != nil {
			return preAccount{}, fmt.Errorf("storage value of slot %s: %w", slot, err)
		}
		pa.storage[slot] = value
	}
	return pa, nil
}

func parseTransaction(tx rawTransaction) (workloadTx, error) {
	sender, err := parseAddress(tx.Sender)
	if err != nil {
		return workloadTx{}, fmt.Errorf("sender: %w", err)
	}
	if tx.SecretKey == "" {
		return workloadTx{}, errors.New("secret_key is required to sign the transaction")
	}
	keyBytes, err := parseBytes(tx.SecretKey)
	if err != nil {
		return workloadTx{}, fmt.Errorf("secret_key: %w", err)
	}
	key, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		return workloadTx{}, fmt.Errorf("secret_key: %w", err)
	}
	if derived := crypto.PubkeyToAddress(key.PublicKey); derived != sender {
		return workloadTx{}, fmt.Errorf("secret_key derives %s but the transaction names sender %s", derived, sender)
	}
	to, err := parseAddress(tx.To)
	if err != nil {
		return workloadTx{}, fmt.Errorf("to: %w", err)
	}
	data, err := parseBytes(tx.Data)
	if err != nil {
		return workloadTx{}, fmt.Errorf("data: %w", err)
	}
	value, err := parseQuantity(tx.Value)
	if err != nil {
		return workloadTx{}, fmt.Errorf("value: %w", err)
	}
	if tx.GasLimit == 0 || tx.GasLimit > params.MaxTxGas {
		return workloadTx{}, fmt.Errorf("gas_limit %d outside (0, %d]", tx.GasLimit, params.MaxTxGas)
	}
	return workloadTx{sender: sender, key: key, to: to, data: data, value: value, gasLimit: tx.GasLimit}, nil
}

func stripHexPrefix(s string) (string, error) {
	if len(s) < 2 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return "", fmt.Errorf("hex value %q must be 0x-prefixed", s)
	}
	return s[2:], nil
}

func parseBytes(s string) ([]byte, error) {
	body, err := stripHexPrefix(s)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("invalid hex byte string %q: %w", s, err)
	}
	return b, nil
}

func parseAddress(s string) (common.Address, error) {
	b, err := parseBytes(s)
	if err != nil {
		return common.Address{}, err
	}
	if len(b) != common.AddressLength {
		return common.Address{}, fmt.Errorf("address %q is %d bytes, want %d", s, len(b), common.AddressLength)
	}
	return common.BytesToAddress(b), nil
}

func parseWord(s string) (common.Hash, error) {
	b, err := parseBytes(s)
	if err != nil {
		return common.Hash{}, err
	}
	if len(b) > common.HashLength {
		return common.Hash{}, fmt.Errorf("word %q is %d bytes, at most %d allowed", s, len(b), common.HashLength)
	}
	return common.BytesToHash(b), nil
}

func parseQuantity(s string) (*big.Int, error) {
	body, err := stripHexPrefix(s)
	if err != nil {
		return nil, err
	}
	if body == "" {
		return nil, fmt.Errorf("empty quantity %q", s)
	}
	v, ok := new(big.Int).SetString(body, 16)
	if !ok {
		return nil, fmt.Errorf("invalid hex quantity %q", s)
	}
	return v, nil
}

func digestHex(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return "0x" + hex.EncodeToString(h.Sum(nil))
}
