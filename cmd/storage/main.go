package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/internal/storage/rwtablebptree"
	testapp_storage "github.com/atkhx/ddb/internal/testapp/storage"
)

var users = makeUsers(5000)

func makeUsers(count int) (res []string) {
	for i := 1; i <= count; i++ {
		res = append(res, fmt.Sprintf("user_%d", i))
	}
	return
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	txFactory := storage.NewTxFactory(0)
	rwTabFactory := rwtablebptree.NewFactory()
	rwTable := rwTabFactory.Create(100)

	txLocks := storage.NewTxLocks(storage.NewTxLockWaitFactory())
	ssTables := storage.NewSSTables()
	txTables := storage.NewTxManager(txFactory, rwTable)

	db := storage.NewStorage(ssTables, txTables, txLocks)

	appstorarge := testapp_storage.New(db)
	appstorarge.GiveFirstAmount(users)

	wg := sync.WaitGroup{}
	rand.Seed(time.Now().UnixNano())
	var timeAvg int64
	var timeAll time.Duration
	txCount := 300_000

	for i := 0; i < txCount; i++ {
		userFrom := users[rand.Intn(len(users))]
		userTo := users[rand.Intn(len(users))]

		if userFrom == userTo {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.Now()
			defer func() {
				duration := time.Now().Sub(t)
				atomic.AddInt64(&timeAvg, int64(duration))
				timeAll += duration

				log.Println("time to tx", duration, "from", userFrom, "to", userTo)
			}()

			appstorarge.SendMoney(userFrom, userTo, int64(10+rand.Intn(90)))
		}()
	}
	wg.Wait()

	log.Println("timeAvg  ", time.Duration(timeAvg/int64(txCount)))
	log.Println("timeAll  ", timeAll)

	appstorarge.CheckTotalAmount(users)
}
