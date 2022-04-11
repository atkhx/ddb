package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/atkhx/ddb/internal/keys"
	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/internal/storage/rwtablebptree"
	"github.com/pkg/errors"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	txFactory := storage.NewTxFactory(0)
	//rwTabFactory := rwtablemap.NewFactory()
	//rwTable := rwTabFactory.Create()
	rwTabFactory := rwtablebptree.NewFactory()
	rwTable := rwTabFactory.Create(100)

	//rwTable := bptree.NewTree(3)

	txLocks := storage.NewTxLocks()
	ssTables := storage.NewSSTables()
	txTables := storage.NewTxManager(txFactory, rwTable)

	db := storage.NewStorage(ssTables, txTables, txLocks)
	giveFirstAmount(db)

	wg := sync.WaitGroup{}
	rand.Seed(time.Now().UnixNano())
	var timeAvg time.Duration
	var timeAll time.Duration

	for i := 0; i < 50000; i++ {
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
				timeAvg += duration
				timeAll += duration

				log.Println("time to tx", duration, "from", userFrom, "to", userTo)
			}()
			sendMoney(db, userFrom, userTo, int64(10+rand.Intn(90)))
		}()
	}
	wg.Wait()

	log.Println("timeAvg  ", timeAvg/50000)
	log.Println("timeAll  ", timeAll)

	checkTotalAmount(db)
}

var users = makeUsers(5000)

type account struct {
	amount int64
}

func makeUsers(count int) (res []string) {
	for i := 1; i <= count; i++ {
		res = append(res, fmt.Sprintf("user_%d", i))
	}
	return
}

func getAccountId(user string) keys.StrKey {
	return keys.StrKey(fmt.Sprintf("account_%s", user))
}

func getAccount(db storage.Storage, tx storage.TxObj, user string) (account, error) {
	accountId := getAccountId(user)
	accountRow, err := db.TxGetForUpdate(tx, accountId)
	//accountRow, err := db.TxGet(tx, accountId)
	if err != nil {
		return account{}, errors.Wrap(err, fmt.Sprintf("get account failed %d %s", tx, accountId))
	} else if accountRow == nil {
		return account{}, errors.New("account not found " + accountId.String())
	}

	result, ok := accountRow.(account)
	if !ok {
		return account{}, errors.New("account type invalid")
	}

	return result, nil
}

func giveFirstAmount(db storage.Storage) {
	var err error
	for _, user := range users {
		accountId := getAccountId(user)
		account := account{amount: 1000}

		if err = db.Set(accountId, account); err != nil {
			err = errors.Wrap(err, "give money for user failed")
			return
		}

		//log.Println("set account", accountId)
	}
}

func checkTotalAmount(db storage.Storage) {
	var amount int64
	for _, user := range users {
		accountRow, err := db.Get(getAccountId(user))
		if err != nil {
			log.Println("get account failed", err)
			return
		}

		account, ok := accountRow.(account)
		if !ok {
			log.Println("invalid account type 1")
			return
		}

		amount += account.amount
	}

	log.Println("amount   ", amount)
	log.Println("expected ", 1000*len(users))
}

func sendMoney(db storage.Storage, fromUser, toUser string, amount int64) {
	tx := db.Begin()
	//tx := db.Begin(storage.ReadCommitted())
	//tx := db.Begin(storage.RepeatableRead())
	var err error
	defer func() {
		if err != nil {
			//if err.Error() != "account FROM has no money" {
			//	log.Println("transaction", tx, "failed with error", err)
			//}
			if err := db.Rollback(tx); err != nil {
				log.Println("rollback transaction", tx, err)
			}
		} else {
			if err := db.Commit(tx); err != nil {
				log.Println("commit transaction failed", err)
			}
		}
	}()

	//if err = db.LockKeys(tx, []internal.Key{getAccountId(fromUser), getAccountId(toUser)}); err != nil {
	//	return
	//}

	accountFrom, err := getAccount(db, tx, fromUser)
	if err != nil {
		//err = errors.Wrap(err, "account FROM not found")
		return
	}

	if accountFrom.amount < amount {
		err = errors.New("account FROM has no money")
		return
	}

	accountTo, err := getAccount(db, tx, toUser)
	if err != nil {
		//err = errors.Wrap(err, "account TO not found")
		return
	}

	accountTo.amount += amount
	accountFrom.amount -= amount

	if err = db.TxSet(tx, getAccountId(toUser), accountTo); err != nil {
		err = errors.Wrap(err, "save account TO failed")
		return
	}

	if err = db.TxSet(tx, getAccountId(fromUser), accountFrom); err != nil {
		err = errors.Wrap(err, "save account FROM failed")
		return
	}
}
