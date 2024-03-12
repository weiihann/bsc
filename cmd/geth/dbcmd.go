// Copyright 2021 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

var (
	removeStateDataFlag = &cli.BoolFlag{
		Name:  "remove.state",
		Usage: "If set, selects the state data for removal",
	}
	removeChainDataFlag = &cli.BoolFlag{
		Name:  "remove.chain",
		Usage: "If set, selects the state data for removal",
	}

	removedbCommand = &cli.Command{
		Action:    removeDB,
		Name:      "removedb",
		Usage:     "Remove blockchain and state databases",
		ArgsUsage: "",
		Flags: flags.Merge(utils.DatabaseFlags,
			[]cli.Flag{removeStateDataFlag, removeChainDataFlag}),
		Description: `
Remove blockchain and state databases`,
	}
	dbCommand = &cli.Command{
		Name:      "db",
		Usage:     "Low level database operations",
		ArgsUsage: "",
		Subcommands: []*cli.Command{
			dbInspectCmd,
			dbStatCmd,
			dbCompactCmd,
			dbGetCmd,
			dbDeleteCmd,
			dbDeleteAllCmd,
			dbInspectTrieCmd,
			dbPutCmd,
			dbGetSlotsCmd,
			dbDumpFreezerIndex,
			dbImportCmd,
			dbExportCmd,
			dbMetadataCmd,
			ancientInspectCmd,
			// no legacy stored receipts for bsc
			// dbMigrateFreezerCmd,
			dbCheckStateContentCmd,
			dbHbss2PbssCmd,
			dbTrieGetCmd,
			dbTrieDeleteCmd,
		},
	}
	dbInspectCmd = &cli.Command{
		Action:    inspect,
		Name:      "inspect",
		ArgsUsage: "<prefix> <start>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "Inspect the storage size for each type of data in the database",
		Description: `This commands iterates the entire database. If the optional 'prefix' and 'start' arguments are provided, then the iteration is limited to the given subset of data.`,
	}
	dbInspectTrieCmd = &cli.Command{
		Action:    inspectTrie,
		Name:      "inspect-trie",
		ArgsUsage: "<blocknum> <jobnum>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
		},
		Usage:       "Inspect the MPT tree of the account and contract.",
		Description: `This commands iterates the entrie WorldState.`,
	}
	dbCheckStateContentCmd = &cli.Command{
		Action:    checkStateContent,
		Name:      "check-state-content",
		ArgsUsage: "<start (optional)>",
		Flags:     flags.Merge(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:     "Verify that state data is cryptographically correct",
		Description: `This command iterates the entire database for 32-byte keys, looking for rlp-encoded trie nodes.
For each trie node encountered, it checks that the key corresponds to the keccak256(value). If this is not true, this indicates
a data corruption.`,
	}
	dbHbss2PbssCmd = &cli.Command{
		Action:    hbss2pbss,
		Name:      "hbss-to-pbss",
		ArgsUsage: "<jobnum (optional)>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.ForceFlag,
			utils.AncientFlag,
		},
		Usage:       "Convert Hash-Base to Path-Base trie node.",
		Description: `This command iterates the entire trie node database and convert the hash-base node to path-base node.`,
	}
	dbTrieGetCmd = &cli.Command{
		Action:    dbTrieGet,
		Name:      "trie-get",
		Usage:     "Show the value of a trie node path key",
		ArgsUsage: "[trie owner] <path-base key>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.BSCMainnetFlag,
			utils.ChapelFlag,
			utils.StateSchemeFlag,
		},
		Description: "This command looks up the specified trie node key from the database.",
	}
	dbTrieDeleteCmd = &cli.Command{
		Action:    dbTrieDelete,
		Name:      "trie-delete",
		Usage:     "delete the specify trie node",
		ArgsUsage: "[trie owner] <hash-base key> | <path-base key>",
		Flags: []cli.Flag{
			utils.DataDirFlag,
			utils.SyncModeFlag,
			utils.BSCMainnetFlag,
			utils.ChapelFlag,
			utils.StateSchemeFlag,
		},
		Description: "This command delete the specify trie node from the database.",
	}
	dbStatCmd = &cli.Command{
		Action: dbStats,
		Name:   "stats",
		Usage:  "Print leveldb statistics",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
	}
	dbCompactCmd = &cli.Command{
		Action: dbCompact,
		Name:   "compact",
		Usage:  "Compact leveldb database. WARNING: May take a very long time",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
			utils.CacheFlag,
			utils.CacheDatabaseFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command performs a database compaction.
WARNING: This operation may take a very long time to finish, and may cause database
corruption if it is aborted during execution'!`,
	}
	dbGetCmd = &cli.Command{
		Action:    dbGet,
		Name:      "get",
		Usage:     "Show the value of a database key",
		ArgsUsage: "<hex-encoded key>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDeleteCmd = &cli.Command{
		Action:    dbDelete,
		Name:      "delete",
		Usage:     "Delete a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command deletes the specified database key from the database.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbDeleteAllCmd = &cli.Command{
		Action:    dbDeleteAll,
		Name:      "delete-all",
		Usage:     "Delete all database keys based on a prefix (WARNING: may corrupt your database)",
		ArgsUsage: "<prefix>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command deletes all keys from the database based on a prefix.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbPutCmd = &cli.Command{
		Action:    dbPut,
		Name:      "put",
		Usage:     "Set the value of a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key> <hex-encoded value>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command sets a given database key to the given value.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbGetSlotsCmd = &cli.Command{
		Action:    dbDumpTrie,
		Name:      "dumptrie",
		Usage:     "Show the storage key/values of a given storage trie",
		ArgsUsage: "<hex-encoded state root> <hex-encoded account hash> <hex-encoded storage trie root> <hex-encoded start (optional)> <int max elements (optional)>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDumpFreezerIndex = &cli.Command{
		Action:    freezerInspect,
		Name:      "freezer-index",
		Usage:     "Dump out the index of a specific freezer table",
		ArgsUsage: "<freezer-type> <table-type> <start (int)> <end (int)>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command displays information about the freezer index.",
	}
	dbImportCmd = &cli.Command{
		Action:    importLDBdata,
		Name:      "import",
		Usage:     "Imports leveldb-data from an exported RLP dump.",
		ArgsUsage: "<dumpfile> <start (optional)",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "The import command imports the specific chain data from an RLP encoded stream.",
	}
	dbExportCmd = &cli.Command{
		Action:    exportChaindata,
		Name:      "export",
		Usage:     "Exports the chain data into an RLP dump. If the <dumpfile> has .gz suffix, gzip compression will be used.",
		ArgsUsage: "<type> <dumpfile>",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Exports the specified chain data to an RLP encoded stream, optionally gzip-compressed.",
	}
	dbMetadataCmd = &cli.Command{
		Action: showMetaData,
		Name:   "metadata",
		Usage:  "Shows metadata about the chain status.",
		Flags: flags.Merge([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Shows metadata about the chain status.",
	}
	ancientInspectCmd = &cli.Command{
		Action: ancientInspect,
		Name:   "inspect-reserved-oldest-blocks",
		Flags: []cli.Flag{
			utils.DataDirFlag,
		},
		Usage: "Inspect the ancientStore information",
		Description: `This commands will read current offset from kvdb, which is the current offset and starting BlockNumber
of ancientStore, will also displays the reserved number of blocks in ancientStore `,
	}
)

func removeDB(ctx *cli.Context) error {
	stack, config := makeConfigNode(ctx)

	// Resolve folder paths.
	var (
		rootDir    = stack.ResolvePath("chaindata")
		ancientDir = config.Eth.DatabaseFreezer
	)
	switch {
	case ancientDir == "":
		ancientDir = filepath.Join(stack.ResolvePath("chaindata"), "ancient")
	case !filepath.IsAbs(ancientDir):
		ancientDir = config.Node.ResolvePath(ancientDir)
	}
	// Delete state data
	statePaths := []string{rootDir, filepath.Join(ancientDir, rawdb.StateFreezerName)}
	confirmAndRemoveDB(statePaths, "state data", ctx, removeStateDataFlag.Name)

	// Delete ancient chain
	chainPaths := []string{filepath.Join(ancientDir, rawdb.ChainFreezerName)}
	confirmAndRemoveDB(chainPaths, "ancient chain", ctx, removeChainDataFlag.Name)
	return nil
}

// removeFolder deletes all files (not folders) inside the directory 'dir' (but
// not files in subfolders).
func removeFolder(dir string) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// If we're at the top level folder, recurse into
		if path == dir {
			return nil
		}
		// Delete all the files, but not subfolders
		if !info.IsDir() {
			os.Remove(path)
			return nil
		}
		return filepath.SkipDir
	})
}

// confirmAndRemoveDB prompts the user for a last confirmation and removes the
// list of folders if accepted.
func confirmAndRemoveDB(paths []string, kind string, ctx *cli.Context, removeFlagName string) {
	var (
		confirm bool
		err     error
	)
	msg := fmt.Sprintf("Location(s) of '%s': \n", kind)
	for _, path := range paths {
		msg += fmt.Sprintf("\t- %s\n", path)
	}
	fmt.Println(msg)
	if ctx.IsSet(removeFlagName) {
		confirm = ctx.Bool(removeFlagName)
		if confirm {
			fmt.Printf("Remove '%s'? [y/n] y\n", kind)
		} else {
			fmt.Printf("Remove '%s'? [y/n] n\n", kind)
		}
	} else {
		confirm, err = prompt.Stdin.PromptConfirm(fmt.Sprintf("Remove '%s'?", kind))
	}
	switch {
	case err != nil:
		utils.Fatalf("%v", err)
	case !confirm:
		log.Info("Database deletion skipped", "kind", kind, "paths", paths)
	default:
		var (
			deleted []string
			start   = time.Now()
		)
		for _, path := range paths {
			if common.FileExist(path) {
				removeFolder(path)
				deleted = append(deleted, path)
			} else {
				log.Info("Folder is not existent", "path", path)
			}
		}
		log.Info("Database successfully deleted", "kind", kind, "paths", deleted, "elapsed", common.PrettyDuration(time.Since(start)))
	}
}

func inspectTrie(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}

	if ctx.NArg() > 3 {
		return fmt.Errorf("Max 3 arguments: %v", ctx.Command.ArgsUsage)
	}

	var (
		blockNumber  uint64
		trieRootHash common.Hash
		jobnum       uint64
	)

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()
	var headerBlockHash common.Hash
	if ctx.NArg() >= 1 {
		if ctx.Args().Get(0) == "latest" {
			headerHash := rawdb.ReadHeadHeaderHash(db)
			blockNumber = *(rawdb.ReadHeaderNumber(db, headerHash))
		} else if ctx.Args().Get(0) == "snapshot" {
			trieRootHash = rawdb.ReadSnapshotRoot(db)
			blockNumber = math.MaxUint64
		} else {
			var err error
			blockNumber, err = strconv.ParseUint(ctx.Args().Get(0), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to Parse blocknum, Args[0]: %v, err: %v", ctx.Args().Get(0), err)
			}
		}

		if ctx.NArg() == 1 {
			jobnum = 1000
		} else {
			var err error
			jobnum, err = strconv.ParseUint(ctx.Args().Get(1), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to Parse jobnum, Args[1]: %v, err: %v", ctx.Args().Get(1), err)
			}
		}

		if blockNumber != math.MaxUint64 {
			headerBlockHash = rawdb.ReadCanonicalHash(db, blockNumber)
			if headerBlockHash == (common.Hash{}) {
				return errors.New("ReadHeadBlockHash empry hash")
			}
			blockHeader := rawdb.ReadHeader(db, headerBlockHash, blockNumber)
			trieRootHash = blockHeader.Root
		}
		if (trieRootHash == common.Hash{}) {
			log.Error("Empty root hash")
		}
		fmt.Printf("ReadBlockHeader, root: %v, blocknum: %v\n", trieRootHash, blockNumber)

		dbScheme := rawdb.ReadStateScheme(db)
		var config *triedb.Config
		if dbScheme == rawdb.PathScheme {
			config = &triedb.Config{
				PathDB: pathdb.ReadOnly,
			}
		} else if dbScheme == rawdb.HashScheme {
			config = triedb.HashDefaults
		}

		triedb := triedb.NewDatabase(db, config)
		theTrie, err := trie.New(trie.TrieID(trieRootHash), triedb)
		if err != nil {
			fmt.Printf("fail to new trie tree, err: %v, rootHash: %v\n", err, trieRootHash.String())
			return err
		}
		theInspect, err := trie.NewInspector(theTrie, triedb, trieRootHash, blockNumber, jobnum)
		if err != nil {
			return err
		}
		theInspect.Run()
		theInspect.DisplayResult()
	}
	return nil
}

func inspect(ctx *cli.Context) error {
	var (
		prefix []byte
		start  []byte
	)
	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 arguments: %v", ctx.Command.ArgsUsage)
	}
	if ctx.NArg() >= 1 {
		if d, err := hexutil.Decode(ctx.Args().Get(0)); err != nil {
			return fmt.Errorf("failed to hex-decode 'prefix': %v", err)
		} else {
			prefix = d
		}
	}
	if ctx.NArg() >= 2 {
		if d, err := hexutil.Decode(ctx.Args().Get(1)); err != nil {
			return fmt.Errorf("failed to hex-decode 'start': %v", err)
		} else {
			start = d
		}
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()

	return rawdb.InspectDatabase(db, prefix, start)
}

func ancientInspect(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, true)
	defer db.Close()
	return rawdb.AncientInspect(db)
}

func checkStateContent(ctx *cli.Context) error {
	var (
		prefix []byte
		start  []byte
	)
	if ctx.NArg() > 1 {
		return fmt.Errorf("max 1 argument: %v", ctx.Command.ArgsUsage)
	}
	if ctx.NArg() > 0 {
		if d, err := hexutil.Decode(ctx.Args().First()); err != nil {
			return fmt.Errorf("failed to hex-decode 'start': %v", err)
		} else {
			start = d
		}
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()
	var (
		it        = rawdb.NewKeyLengthIterator(db.NewIterator(prefix, start), 32)
		hasher    = crypto.NewKeccakState()
		got       = make([]byte, 32)
		errs      int
		count     int
		startTime = time.Now()
		lastLog   = time.Now()
	)
	for it.Next() {
		count++
		k := it.Key()
		v := it.Value()
		hasher.Reset()
		hasher.Write(v)
		hasher.Read(got)
		if !bytes.Equal(k, got) {
			errs++
			fmt.Printf("Error at %#x\n", k)
			fmt.Printf("  Hash:  %#x\n", got)
			fmt.Printf("  Data:  %#x\n", v)
		}
		if time.Since(lastLog) > 8*time.Second {
			log.Info("Iterating the database", "at", fmt.Sprintf("%#x", k), "elapsed", common.PrettyDuration(time.Since(startTime)))
			lastLog = time.Now()
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	log.Info("Iterated the state content", "errors", errs, "items", count)
	return nil
}

func showLeveldbStats(db ethdb.KeyValueStater) {
	if stats, err := db.Stat("leveldb.stats"); err != nil {
		log.Warn("Failed to read database stats", "error", err)
	} else {
		fmt.Println(stats)
	}
	if ioStats, err := db.Stat("leveldb.iostats"); err != nil {
		log.Warn("Failed to read database iostats", "error", err)
	} else {
		fmt.Println(ioStats)
	}
}

func dbStats(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()

	showLeveldbStats(db)
	if db.StateStore() != nil {
		fmt.Println("show stats of state store")
		showLeveldbStats(db.StateStore())
	}

	return nil
}

func dbCompact(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false, false)
	defer db.Close()

	log.Info("Stats before compaction")
	showLeveldbStats(db)

	statediskdb := db.StateStore()
	if statediskdb != nil {
		fmt.Println("show stats of state store")
		showLeveldbStats(statediskdb)
	}

	log.Info("Triggering compaction")
	if err := db.Compact(nil, nil); err != nil {
		log.Error("Compact err", "error", err)
		return err
	}

	if statediskdb != nil {
		if err := statediskdb.Compact(nil, nil); err != nil {
			log.Error("Compact err", "error", err)
			return err
		}
	}

	log.Info("Stats after compaction")
	showLeveldbStats(db)
	if statediskdb != nil {
		fmt.Println("show stats of state store after compaction")
		showLeveldbStats(statediskdb)
	}
	return nil
}

// dbGet shows the value of a given database key
func dbGet(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()

	key, err := common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}

	statediskdb := db.StateStore()
	data, err := db.Get(key)
	if err != nil {
		// if separate trie db exist, try to get it from separate db
		if statediskdb != nil {
			statedata, dberr := statediskdb.Get(key)
			if dberr == nil {
				fmt.Printf("key %#x: %#x\n", key, statedata)
				return nil
			}
		}
		log.Info("Get operation failed", "key", fmt.Sprintf("%#x", key), "error", err)
		return err
	}
	fmt.Printf("key %#x: %#x\n", key, data)
	return nil
}

// dbTrieGet shows the value of a given database key
func dbTrieGet(ctx *cli.Context) error {
	if ctx.NArg() < 1 || ctx.NArg() > 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	var db ethdb.Database
	chaindb := utils.MakeChainDatabase(ctx, stack, true, false)
	if chaindb.StateStore() != nil {
		db = chaindb.StateStore()
	} else {
		db = chaindb
	}
	defer chaindb.Close()

	scheme := ctx.String(utils.StateSchemeFlag.Name)
	if scheme == "" {
		scheme = rawdb.HashScheme
	}

	if scheme == rawdb.PathScheme {
		var (
			pathKey []byte
			owner   []byte
			err     error
		)
		if ctx.NArg() == 1 {
			pathKey, err = hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			nodeVal, hash := rawdb.ReadAccountTrieNode(db, pathKey)
			log.Info("TrieGet result ", "PathKey", common.Bytes2Hex(pathKey), "Hash: ", hash, "node: ", trie.NodeString(hash.Bytes(), nodeVal))
		} else if ctx.NArg() == 2 {
			owner, err = hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			pathKey, err = hexutil.Decode(ctx.Args().Get(1))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}

			nodeVal, hash := rawdb.ReadStorageTrieNode(db, common.BytesToHash(owner), pathKey)
			log.Info("TrieGet result ", "PathKey: ", common.Bytes2Hex(pathKey), "Owner: ", common.BytesToHash(owner), "Hash: ", hash, "node: ", trie.NodeString(hash.Bytes(), nodeVal))
		}
	} else if scheme == rawdb.HashScheme {
		if ctx.NArg() == 1 {
			hashKey, err := hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			val, err := db.Get(hashKey)
			if err != nil {
				log.Error("db get failed, ", "error: ", err)
				return err
			}
			log.Info("TrieGet result ", "HashKey: ", common.BytesToHash(hashKey), "node: ", trie.NodeString(hashKey, val))
		} else {
			log.Error("args too much")
		}
	}

	return nil
}

// dbTrieDelete delete the trienode of a given database key
func dbTrieDelete(ctx *cli.Context) error {
	if ctx.NArg() < 1 || ctx.NArg() > 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	var db ethdb.Database
	chaindb := utils.MakeChainDatabase(ctx, stack, true, false)
	if chaindb.StateStore() != nil {
		db = chaindb.StateStore()
	} else {
		db = chaindb
	}
	defer chaindb.Close()

	scheme := ctx.String(utils.StateSchemeFlag.Name)
	if scheme == "" {
		scheme = rawdb.HashScheme
	}

	if scheme == rawdb.PathScheme {
		var (
			pathKey []byte
			owner   []byte
			err     error
		)
		if ctx.NArg() == 1 {
			pathKey, err = hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			rawdb.DeleteAccountTrieNode(db, pathKey)
		} else if ctx.NArg() == 2 {
			owner, err = hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			pathKey, err = hexutil.Decode(ctx.Args().Get(1))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			rawdb.DeleteStorageTrieNode(db, common.BytesToHash(owner), pathKey)
		}
	} else if scheme == rawdb.HashScheme {
		if ctx.NArg() == 1 {
			hashKey, err := hexutil.Decode(ctx.Args().Get(0))
			if err != nil {
				log.Info("Could not decode the value", "error", err)
				return err
			}
			err = db.Delete(hashKey)
			if err != nil {
				log.Error("db delete failed", "err", err)
				return err
			}
		} else {
			log.Error("args too much")
		}
	}
	return nil
}

// dbDelete deletes a key from the database
func dbDelete(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false, false)
	defer db.Close()

	key, err := common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	data, err := db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	if err = db.Delete(key); err != nil {
		log.Info("Delete operation returned an error", "key", fmt.Sprintf("%#x", key), "error", err)
		return err
	}
	return nil
}

// dbDeleteAll deletes all keys based on a prefix key from the database
func dbDeleteAll(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}

	prefixKey, err := common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		return fmt.Errorf("failed to hex-decode 'prefix': %v", err)
	}

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false, false)
	defer db.Close()

	it := db.NewIterator(prefixKey, nil)
	defer it.Release()

	var (
		start  = time.Now()
		logged = time.Now()
		batch  = db.NewBatch()
		count  int64
		key    []byte
	)

	for it.Next() {
		key = it.Key()
		if err = batch.Delete(key); err != nil {
			log.Error("Delete operation returned an error", "key", fmt.Sprintf("%#x", key), "error", err)
			return err
		}

		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err = batch.Write(); err != nil {
				log.Error("Write batch operation returned an error", "error", err)
				return err
			}
			batch.Reset()
		}

		count++
		if time.Since(logged) > 8*time.Second {
			log.Info("Deleting keys", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}

	if batch.ValueSize() > 0 {
		if err = batch.Write(); err != nil {
			log.Error("Write batch operation returned an error", "error", err)
			return err
		}
		batch.Reset()
	}

	log.Info(fmt.Sprintf("Deleted all keys at prefix '%v'", prefixKey), "count", count, "elapsed", common.PrettyDuration(time.Since(start)))

	return nil
}

// dbPut overwrite a value in the database
func dbPut(ctx *cli.Context) error {
	if ctx.NArg() != 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false, false)
	defer db.Close()

	var (
		key   []byte
		value []byte
		data  []byte
		err   error
	)
	key, err = common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	value, err = hexutil.Decode(ctx.Args().Get(1))
	if err != nil {
		log.Info("Could not decode the value", "error", err)
		return err
	}
	data, err = db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	return db.Put(key, value)
}

// dbDumpTrie shows the key-value slots of a given storage trie
func dbDumpTrie(ctx *cli.Context) error {
	if ctx.NArg() < 3 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()

	triedb := utils.MakeTrieDatabase(ctx, db, false, true, false)
	defer triedb.Close()

	var (
		state   []byte
		storage []byte
		account []byte
		start   []byte
		max     = int64(-1)
		err     error
	)
	if state, err = hexutil.Decode(ctx.Args().Get(0)); err != nil {
		log.Info("Could not decode the state root", "error", err)
		return err
	}
	if account, err = hexutil.Decode(ctx.Args().Get(1)); err != nil {
		log.Info("Could not decode the account hash", "error", err)
		return err
	}
	if storage, err = hexutil.Decode(ctx.Args().Get(2)); err != nil {
		log.Info("Could not decode the storage trie root", "error", err)
		return err
	}
	if ctx.NArg() > 3 {
		if start, err = hexutil.Decode(ctx.Args().Get(3)); err != nil {
			log.Info("Could not decode the seek position", "error", err)
			return err
		}
	}
	if ctx.NArg() > 4 {
		if max, err = strconv.ParseInt(ctx.Args().Get(4), 10, 64); err != nil {
			log.Info("Could not decode the max count", "error", err)
			return err
		}
	}
	id := trie.StorageTrieID(common.BytesToHash(state), common.BytesToHash(account), common.BytesToHash(storage))
	theTrie, err := trie.New(id, triedb)
	if err != nil {
		return err
	}
	trieIt, err := theTrie.NodeIterator(start)
	if err != nil {
		return err
	}
	var count int64
	it := trie.NewIterator(trieIt)
	for it.Next() {
		if max > 0 && count == max {
			fmt.Printf("Exiting after %d values\n", count)
			break
		}
		fmt.Printf("  %d. key %#x: %#x\n", count, it.Key, it.Value)
		count++
	}
	return it.Err
}

func freezerInspect(ctx *cli.Context) error {
	if ctx.NArg() < 4 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		freezer = ctx.Args().Get(0)
		table   = ctx.Args().Get(1)
	)
	start, err := strconv.ParseInt(ctx.Args().Get(2), 10, 64)
	if err != nil {
		log.Info("Could not read start-param", "err", err)
		return err
	}
	end, err := strconv.ParseInt(ctx.Args().Get(3), 10, 64)
	if err != nil {
		log.Info("Could not read count param", "err", err)
		return err
	}
	stack, _ := makeConfigNode(ctx)
	ancient := stack.ResolveAncient("chaindata", ctx.String(utils.AncientFlag.Name))
	stack.Close()
	return rawdb.InspectFreezerTable(ancient, freezer, table, start, end)
}

func importLDBdata(ctx *cli.Context) error {
	start := 0
	switch ctx.NArg() {
	case 1:
		break
	case 2:
		s, err := strconv.Atoi(ctx.Args().Get(1))
		if err != nil {
			return fmt.Errorf("second arg must be an integer: %v", err)
		}
		start = s
	default:
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		fName     = ctx.Args().Get(0)
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during ldb import, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, false, false)
	defer db.Close()
	return utils.ImportLDBData(db, fName, int64(start), stop)
}

type preimageIterator struct {
	iter ethdb.Iterator
}

func (iter *preimageIterator) Next() (byte, []byte, []byte, bool) {
	for iter.iter.Next() {
		key := iter.iter.Key()
		if bytes.HasPrefix(key, rawdb.PreimagePrefix) && len(key) == (len(rawdb.PreimagePrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.iter.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *preimageIterator) Release() {
	iter.iter.Release()
}

type snapshotIterator struct {
	init    bool
	account ethdb.Iterator
	storage ethdb.Iterator
}

func (iter *snapshotIterator) Next() (byte, []byte, []byte, bool) {
	if !iter.init {
		iter.init = true
		return utils.OpBatchDel, rawdb.SnapshotRootKey, nil, true
	}
	for iter.account.Next() {
		key := iter.account.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotAccountPrefix) && len(key) == (len(rawdb.SnapshotAccountPrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.account.Value(), true
		}
	}
	for iter.storage.Next() {
		key := iter.storage.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotStoragePrefix) && len(key) == (len(rawdb.SnapshotStoragePrefix)+2*common.HashLength) {
			return utils.OpBatchAdd, key, iter.storage.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *snapshotIterator) Release() {
	iter.account.Release()
	iter.storage.Release()
}

// chainExporters defines the export scheme for all exportable chain data.
var chainExporters = map[string]func(db ethdb.Database) utils.ChainDataIterator{
	"preimage": func(db ethdb.Database) utils.ChainDataIterator {
		iter := db.NewIterator(rawdb.PreimagePrefix, nil)
		return &preimageIterator{iter: iter}
	},
	"snapshot": func(db ethdb.Database) utils.ChainDataIterator {
		account := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
		storage := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
		return &snapshotIterator{account: account, storage: storage}
	},
}

func exportChaindata(ctx *cli.Context) error {
	if ctx.NArg() < 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	// Parse the required chain data type, make sure it's supported.
	kind := ctx.Args().Get(0)
	kind = strings.ToLower(strings.Trim(kind, " "))
	exporter, ok := chainExporters[kind]
	if !ok {
		var kinds []string
		for kind := range chainExporters {
			kinds = append(kinds, kind)
		}
		return fmt.Errorf("invalid data type %s, supported types: %s", kind, strings.Join(kinds, ", "))
	}
	var (
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during db export, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()
	return utils.ExportChaindata(ctx.Args().Get(1), kind, exporter(db), stop)
}

func showMetaData(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()
	db := utils.MakeChainDatabase(ctx, stack, true, false)
	defer db.Close()
	ancients, err := db.Ancients()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error accessing ancients: %v", err)
	}
	data := rawdb.ReadChainMetadata(db)
	data = append(data, []string{"frozen", fmt.Sprintf("%d items", ancients)})
	data = append(data, []string{"snapshotGenerator", snapshot.ParseGeneratorStatus(rawdb.ReadSnapshotGenerator(db))})
	if b := rawdb.ReadHeadBlock(db); b != nil {
		data = append(data, []string{"headBlock.Hash", fmt.Sprintf("%v", b.Hash())})
		data = append(data, []string{"headBlock.Root", fmt.Sprintf("%v", b.Root())})
		data = append(data, []string{"headBlock.Number", fmt.Sprintf("%d (%#x)", b.Number(), b.Number())})
	}
	if h := rawdb.ReadHeadHeader(db); h != nil {
		data = append(data, []string{"headHeader.Hash", fmt.Sprintf("%v", h.Hash())})
		data = append(data, []string{"headHeader.Root", fmt.Sprintf("%v", h.Root)})
		data = append(data, []string{"headHeader.Number", fmt.Sprintf("%d (%#x)", h.Number, h.Number)})
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Field", "Value"})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func hbss2pbss(ctx *cli.Context) error {
	if ctx.NArg() > 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}

	var jobnum uint64
	var err error
	if ctx.NArg() == 1 {
		jobnum, err = strconv.ParseUint(ctx.Args().Get(0), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to Parse jobnum, Args[1]: %v, err: %v", ctx.Args().Get(1), err)
		}
	} else {
		// by default
		jobnum = 1000
	}

	force := ctx.Bool(utils.ForceFlag.Name)

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false, false)
	db.Sync()
	stateDiskDb := db.StateStore()
	defer db.Close()

	// convert hbss trie node to pbss trie node
	var lastStateID uint64
	if stateDiskDb != nil {
		lastStateID = rawdb.ReadPersistentStateID(stateDiskDb)
	} else {
		lastStateID = rawdb.ReadPersistentStateID(db)
	}
	if lastStateID == 0 || force {
		config := triedb.HashDefaults
		triedb := triedb.NewDatabase(db, config)
		triedb.Cap(0)
		log.Info("hbss2pbss triedb", "scheme", triedb.Scheme())
		defer triedb.Close()

		headerHash := rawdb.ReadHeadHeaderHash(db)
		blockNumber := rawdb.ReadHeaderNumber(db, headerHash)
		if blockNumber == nil {
			log.Error("read header number failed.")
			return fmt.Errorf("read header number failed")
		}

		log.Info("hbss2pbss converting", "HeaderHash: ", headerHash.String(), ", blockNumber: ", *blockNumber)

		var headerBlockHash common.Hash
		var trieRootHash common.Hash

		if *blockNumber != math.MaxUint64 {
			headerBlockHash = rawdb.ReadCanonicalHash(db, *blockNumber)
			if headerBlockHash == (common.Hash{}) {
				return errors.New("ReadHeadBlockHash empty hash")
			}
			blockHeader := rawdb.ReadHeader(db, headerBlockHash, *blockNumber)
			trieRootHash = blockHeader.Root
			fmt.Println("Canonical Hash: ", headerBlockHash.String(), ", TrieRootHash: ", trieRootHash.String())
		}
		if (trieRootHash == common.Hash{}) {
			log.Error("Empty root hash")
			return errors.New("Empty root hash.")
		}

		id := trie.StateTrieID(trieRootHash)
		theTrie, err := trie.New(id, triedb)
		if err != nil {
			log.Error("fail to new trie tree", "err", err, "rootHash", err, trieRootHash.String())
			return err
		}

		h2p, err := trie.NewHbss2Pbss(theTrie, triedb, trieRootHash, *blockNumber, jobnum)
		if err != nil {
			log.Error("fail to new hash2pbss", "err", err, "rootHash", err, trieRootHash.String())
			return err
		}
		h2p.Run()
	} else {
		log.Info("Convert hbss to pbss success. Nothing to do.")
	}

	// repair state ancient offset
	if stateDiskDb != nil {
		lastStateID = rawdb.ReadPersistentStateID(stateDiskDb)
	} else {
		lastStateID = rawdb.ReadPersistentStateID(db)
	}

	if lastStateID == 0 {
		log.Error("Convert hbss to pbss trie node error. The last state id is still 0")
	}

	var ancient string
	if db.StateStore() != nil {
		dirName := filepath.Join(stack.ResolvePath("chaindata"), "state")
		ancient = filepath.Join(dirName, "ancient")
	} else {
		ancient = stack.ResolveAncient("chaindata", ctx.String(utils.AncientFlag.Name))
	}
	err = rawdb.ResetStateFreezerTableOffset(ancient, lastStateID)
	if err != nil {
		log.Error("Reset state freezer table offset failed", "error", err)
		return err
	}
	// prune hbss trie node
	if stateDiskDb != nil {
		err = rawdb.PruneHashTrieNodeInDataBase(stateDiskDb)
	} else {
		err = rawdb.PruneHashTrieNodeInDataBase(db)
	}
	if err != nil {
		log.Error("Prune Hash trie node in database failed", "error", err)
		return err
	}
	return nil
}
