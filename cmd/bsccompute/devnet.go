package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/parlia"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/systemcontracts"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

//go:embed genesis-dev-bench.json
var genesisJSON []byte

const (
	headerGasLimit     uint64 = 70_000_000
	measuredNumber     uint64 = 10
	validatorSecretHex        = "6dc1dae2df4df63adbe572ebd31c9af6dd8d2c77cd82788ca45f80230d3db175"
	validatorBLSHex           = "97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb"
	genesisExtraLength        = 32 + common.AddressLength + 65
)

var (
	validatorAddress = common.HexToAddress("0x8F3906Aa02F00b69148f667aeAf72a8375A31d85")
	workloadTip      = big.NewInt(params.GWei)
)

type devnet struct {
	genesis    *core.Genesis
	genesisSHA [32]byte
	config     *params.ChainConfig
	validator  *ecdsa.PrivateKey
	coinbase   common.Address
}

func loadDevnet() (*devnet, error) {
	var g core.Genesis
	if err := json.Unmarshal(genesisJSON, &g); err != nil {
		return nil, fmt.Errorf("decoding embedded genesis: %w", err)
	}
	if g.Config == nil || g.Config.Parlia == nil {
		return nil, errors.New("embedded genesis is not a Parlia chain")
	}
	cfg := *g.Config
	cfg.PasteurTime = new(uint64)
	blobs := *params.BSCChainConfig.BlobScheduleConfig
	cfg.BlobScheduleConfig = &blobs
	if err := checkForkParity(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.CheckConfigForkOrder(); err != nil {
		return nil, fmt.Errorf("devnet fork order: %w", err)
	}
	g.Config = &cfg
	g.GasLimit = headerGasLimit

	key, err := crypto.HexToECDSA(validatorSecretHex)
	if err != nil {
		return nil, fmt.Errorf("validator key: %w", err)
	}
	if addr := crypto.PubkeyToAddress(key.PublicKey); addr != validatorAddress {
		return nil, fmt.Errorf("validator key derives %s, want %s", addr, validatorAddress)
	}
	if len(g.ExtraData) != genesisExtraLength || !bytes.Equal(g.ExtraData[32:32+common.AddressLength], validatorAddress.Bytes()) {
		return nil, fmt.Errorf("genesis extraData does not name the single validator %s", validatorAddress)
	}
	if acct, ok := g.Alloc[validatorAddress]; !ok || acct.Balance == nil || acct.Balance.Sign() <= 0 {
		return nil, fmt.Errorf("genesis does not fund validator %s", validatorAddress)
	}
	bls, err := hex.DecodeString(validatorBLSHex)
	if err != nil {
		return nil, fmt.Errorf("validator BLS key: %w", err)
	}
	set, ok := g.Alloc[common.HexToAddress(systemcontracts.ValidatorContract)]
	if !ok || !bytes.Contains(set.Code, validatorAddress.Bytes()) || !bytes.Contains(set.Code, bls) {
		return nil, errors.New("genesis ValidatorSet code does not embed the deterministic validator and BLS key")
	}
	return &devnet{
		genesis:    &g,
		genesisSHA: sha256.Sum256(genesisJSON),
		config:     &cfg,
		validator:  key,
		coinbase:   validatorAddress,
	}, nil
}

func checkForkParity(cfg *params.ChainConfig) error {
	timeType := reflect.TypeOf((*uint64)(nil))
	blockType := reflect.TypeOf((*big.Int)(nil))
	want := reflect.ValueOf(params.BSCChainConfig).Elem()
	have := reflect.ValueOf(cfg).Elem()
	for i := range want.NumField() {
		field := want.Type().Field(i)
		switch {
		case field.Type == timeType && strings.HasSuffix(field.Name, "Time"):
			w := want.Field(i).Interface().(*uint64)
			h := have.Field(i).Interface().(*uint64)
			if w == nil && h != nil {
				return fmt.Errorf("fork %s is unscheduled on BSC mainnet but scheduled at %d", field.Name, *h)
			}
			if w != nil && (h == nil || *h != 0) {
				return fmt.Errorf("fork %s is active on BSC mainnet and must be active from genesis", field.Name)
			}
		case field.Type == blockType && strings.HasSuffix(field.Name, "Block"):
			w := want.Field(i).Interface().(*big.Int)
			h := have.Field(i).Interface().(*big.Int)
			if (w == nil) != (h == nil) {
				return fmt.Errorf("fork %s presence differs from BSC mainnet", field.Name)
			}
			if h != nil && (!h.IsUint64() || h.Uint64() >= measuredNumber) {
				return fmt.Errorf("fork %s activates at %s, not before measured block %d", field.Name, h, measuredNumber)
			}
		}
	}
	return nil
}

func (d *devnet) caseGenesis(wc *workloadCase) (*core.Genesis, error) {
	alloc := make(types.GenesisAlloc, len(d.genesis.Alloc)+len(wc.pre))
	for addr, acct := range d.genesis.Alloc {
		alloc[addr] = acct
	}
	for addr, pa := range wc.pre {
		if _, taken := alloc[addr]; taken || addr == params.HistoryStorageAddress {
			return nil, fmt.Errorf("pre account %s collides with a devnet system, validator or reserved account", addr)
		}
		alloc[addr] = types.Account{Code: pa.code, Storage: pa.storage, Balance: pa.balance, Nonce: pa.nonce}
	}
	g := *d.genesis
	g.Alloc = alloc
	return &g, nil
}

func (d *devnet) authorize(engine *parlia.Parlia) {
	key := d.validator
	engine.Authorize(d.coinbase,
		func(_ accounts.Account, _ string, data []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(data), key)
		},
		func(_ accounts.Account, tx *types.Transaction, chainID *big.Int) (*types.Transaction, error) {
			return types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
		},
	)
}
