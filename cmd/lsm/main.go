package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/atkhx/ddb/pkg/key"
	"github.com/atkhx/ddb/pkg/lsm/filemanager"
	"github.com/atkhx/ddb/pkg/lsm/flusher"
	ssConstructor "github.com/atkhx/ddb/pkg/lsm/sstable/constructor"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/atkhx/ddb/pkg/lsm/tree"
	"github.com/atkhx/ddb/pkg/row"
	"github.com/atkhx/ddb/pkg/sorter-rows-slice"
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/atkhx/ddb/pkg/walog/filelogger"
	"github.com/atkhx/ddb/pkg/walog/filescanner"
	"github.com/pkg/errors"
)

const (
	walCapacity = 2 * 1024
	memCapacity = 2 * 1024

	dataPath    = "./data"
	dataPathSS  = dataPath + "/ss"
	dataPathWAL = dataPath + "/wal"
)

type Tree interface {
	Search(k key.Key) (storage.Row, error)
	Insert(r storage.Row) error
	Delete(k key.Key) error
}

var (
	rowUnSerializer    = row.NewUnSerializer()
	fileManager        = filemanager.New(dataPathSS)
	ssTableConstructor = ssConstructor.New(fileManager, rowUnSerializer)
	memFlusher         = flusher.New(ssTableConstructor, fileManager)
	walogWriter        = filelogger.New(dataPathWAL, walCapacity)
)

func main() {
	if err := os.MkdirAll(dataPathSS, os.ModePerm); err != nil {
		log.Fatalln("create path for ssTables files failed with error", err)
	}

	if err := os.MkdirAll(dataPathWAL, os.ModePerm); err != nil {
		log.Fatalln("create path for wal files failed with error", err)
	}

	defer walogWriter.Close()

	// A1 Сканирование записанных ранее WAL-файлов в inmem
	walFiles, err := filescanner.GetWalFiles(dataPathWAL)
	if err != nil {
		log.Fatalln(err)
	}
	walogScanner := filescanner.New(walFiles)

	memLength := 0
	memTable := sorter_rows_slice.NewItemsRowsSlice()

	err = walogScanner.Scan(func(record walog.Record) error {
		memLength += len(record.Data)

		r, err := rowUnSerializer.RowFromBytes(record.Data)
		if err != nil {
			return errors.Wrap(err, "unserialize row failed")
		}
		return memTable.Insert(r)
	})

	// B1 Сканирование сохраненных файлов SS-таблиц
	ssTables, err := ssTableConstructor.InitSSTables()
	if err != nil {
		log.Fatalln(errors.Wrap(err, "init ssTables failed"))
	}

	lsmStorage := storage.New(memTable, memLength, memCapacity, memFlusher, ssTables, walogWriter)
	if err != nil {
		log.Fatalln(err)
	}
	defer lsmStorage.Close()

	// A2 Сохранение inmem и удаление WAL-файлов
	if err := lsmStorage.Flush(); err != nil {
		log.Fatalln("flush storage failed", err)
	} else if err = walogScanner.Clean(); err != nil {
		log.Fatalln("flush walogScanner failed", err)
	}

	newTree := tree.NewTree(lsmStorage)

	writeRecords(newTree)
	readRecords(newTree)
}

func writeRecords(lsmTree Tree) {
	rand.Seed(time.Now().UnixNano())

	for i := 1; i <= 50; i++ {
		//k := rand.Intn(100)
		k := key.IntKey(i)
		r := row.New(k, fmt.Sprintf("row data for index %d", k))

		if err := lsmTree.Insert(r); err != nil {
			log.Fatalln(err)
		}
	}

	if err := lsmTree.Delete(key.IntKey(45)); err != nil {
		log.Fatalln(err)
	}

	if err := lsmTree.Delete(key.IntKey(31)); err != nil {
		log.Fatalln(err)
	}

	if err := lsmTree.Insert(row.New(key.IntKey(45), fmt.Sprintf("new row data for index %d", 45))); err != nil {
		log.Fatalln(err)
	}
}

func readRecords(lsmTree Tree) {
	for i := 1; i <= 50; i++ {
		//k := rand.Intn(100)
		k := i

		r, err := lsmTree.Search(key.IntKey(k))
		if err != nil {
			log.Fatalln(err)
		}

		if r == nil {
			fmt.Println("not found by key", k, "row", r)
		} else {
			fmt.Println("found by key", k, "row", r)
		}
	}
}
