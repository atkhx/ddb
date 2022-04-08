package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/internal/storage/rwtablemap"
	"github.com/pkg/errors"
)

func main() {
	txFactory := storage.NewTxFactory(0)
	rwTabFactory := rwtablemap.NewFactory()

	txLocks := storage.NewTxLocks()
	ssTables := storage.NewSSTables()
	txTables := storage.NewTxManager(storage.NewReadCommitted(), txFactory, rwTabFactory)

	//go scheduleVacuum(10*time.Millisecond, txTables)
	//go schedulePersister(100*time.Millisecond, txTables, ssTables)

	db := storage.NewStorage(ssTables, txTables, txLocks)
	giveFirstAmount(db)

	wg := sync.WaitGroup{}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 5000; i++ {
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
				log.Println("time to tx", time.Now().Sub(t), "from", userFrom, "to", userTo)
			}()
			sendMoney(db, userFrom, userTo, int64(10+rand.Intn(90)))
		}()
	}
	wg.Wait()

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

func getAccountId(user string) string {
	return fmt.Sprintf("account_%s", user)
}

func getAccount(db storage.Storage, tx storage.TxObj, user string) (account, error) {
	accountId := getAccountId(user)
	accountRow, err := db.TxGetForUpdate(tx, accountId)
	//accountRow, err := db.TxGet(tx, accountId)
	if err != nil {
		return account{}, errors.Wrap(err, fmt.Sprintf("get account failed %d %s", tx, accountId))
	} else if accountRow == nil {
		return account{}, errors.New("account not found " + accountId)
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

		log.Println("set account", accountId)
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
			log.Println("invalid account type")
			return
		}

		amount += account.amount
	}

	log.Println("amount   ", amount)
	log.Println("expected ", 1000*len(users))
}

func sendMoney(db storage.Storage, fromUser, toUser string, amount int64) {
	tx := db.Begin()
	var err error
	defer func() {
		if err != nil {
			if err.Error() != "account FROM has no money" {
				log.Println("transaction", tx, "failed with error", err)
			}
			log.Println("rollback transaction", tx, db.Rollback(tx), err)
		} else {
			if err := db.Commit(tx); err != nil {
				log.Println("commit transaction failed", err)
			}
		}
	}()

	accountFrom, err := getAccount(db, tx, fromUser)
	if err != nil {
		err = errors.Wrap(err, "account FROM not found")
		return
	}

	if accountFrom.amount < amount {
		err = errors.New("account FROM has no money")
		return
	}

	accountTo, err := getAccount(db, tx, toUser)
	if err != nil {
		err = errors.Wrap(err, "account TO not found")
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

func scheduleVacuum(delay time.Duration, txTables storage.TxManager) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			txTables.Vacuum()
			timer.Reset(delay)
		}
	}
}

func schedulePersister(delay time.Duration, txTables storage.TxManager, ssTables storage.ROTables) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			txTables.Persist(func(table storage.RWTable) error {
				ssTables.Grow(table)
				return nil
			})
			timer.Reset(delay)
		}
	}
}
