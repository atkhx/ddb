package main

import (
	"log"
	"strings"
	"time"

	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/internal/storage/rwtablemap"
)

func main() {
	txFactory := storage.NewTxFactory(time.Now().UnixNano())
	rwTabFactory := rwtablemap.NewFactory()

	txLocks := storage.NewTxLocks()
	ssTables := storage.NewSSTables()
	txTables := storage.NewTxTables(txLocks, storage.NewTxAccess(), txFactory, rwTabFactory)

	db := storage.NewStorage(ssTables, txTables)
	txID := db.Begin()

	log.Println(strings.Repeat("-", 30))
	log.Println("get row by key 1")
	r, err := db.Get(1)
	if err != nil {
		log.Println("error:", err)
		log.Println("rollback", db.Rollback(txID))
		return
	}
	log.Println("value:", r)

	log.Println(strings.Repeat("-", 30))
	log.Println("set row by key 1")

	if err := db.Set(1, "row value"); err != nil {
		log.Println("error:", err)
		log.Println("rollback", db.Rollback(txID))
		return
	}

	log.Println(strings.Repeat("-", 30))
	log.Println("get row by key 1")
	r, err = db.Get(1)
	if err != nil {
		log.Println("error:", err)
		log.Println("rollback", db.Rollback(txID))
		return
	}
	log.Println("value:", r)

	log.Println(strings.Repeat("-", 30))
	log.Println("get row by key 1 in not committed tx")
	r, err = db.TxGet(txID, 1)
	if err != nil {
		log.Println("error:", err)
		log.Println("rollback", db.Rollback(txID))
		return
	}
	log.Println("value:", r)
	log.Println("commit", db.Commit(txID))
}
