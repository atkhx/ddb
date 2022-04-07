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
	//txFactory := storage.NewTxFactory(time.Now().UnixNano())
	txFactory := storage.NewTxFactory(0)
	rwTabFactory := rwtablemap.NewFactory()

	txLocks := storage.NewTxLocks()
	ssTables := storage.NewSSTables()
	txTables := storage.NewTxTables(storage.NewTxAccess(), txFactory, rwTabFactory)

	go scheduleVacuum(10*time.Millisecond, txTables)
	//go schedulePersister(100*time.Millisecond, txTables, ssTables)

	db := storage.NewStorage(ssTables, txTables, txLocks)
	giveFirstAmount(db)

	wg := sync.WaitGroup{}
	//rand.Seed(123)
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

var users = []string{"user1", "user2", "user3", "user4", "user5"}

type account struct {
	amount int64
}

func getAccountId(user string) string {
	return fmt.Sprintf("account_%s", user)
}

func getAccount(db Storage, tx int64, user string) (account, error) {
	accountId := getAccountId(user)
	accountRow, err := db.TxGetForUpdate(tx, true, accountId)
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

func giveFirstAmount(db Storage) {
	tx := db.Begin()
	var err error
	defer func() {
		if err != nil {
			log.Println("transaction", tx, "failed with error", err)
			log.Println("rollback transaction", db.Rollback(tx))
		} else {
			log.Println("commit transaction", db.Commit(tx))
		}
	}()

	for _, user := range users {
		accountId := getAccountId(user)
		account := account{amount: 1000}

		if err = db.TxSet(tx, accountId, account); err != nil {
			err = errors.Wrap(err, "give money for user failed")
			return
		}

		log.Println("set account", accountId)
	}
}

func checkTotalAmount(db Storage) {
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

func sendMoney(db Storage, fromUser, toUser string, amount int64) {
	tx := db.Begin()
	var err error
	defer func() {
		if err != nil {
			if err.Error() != "account FROM has no money" {
				log.Println("transaction", tx, "failed with error", err)
			}
			log.Println("rollback transaction", tx, db.Rollback(tx))
			//if err := db.Rollback(tx); err != nil {
			//	log.Println("rollback failed", err)
			//}
		} else {
			//log.Println("send", amount, "from", fromUser, "to", toUser, "success")
			if err := db.Commit(tx); err != nil {
				log.Println("commit transaction failed", err)
			}
		}
	}()

	if err = db.LockKeys(tx, true, []storage.Key{fromUser, toUser}); err != nil {
		return
	}

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

type Storage interface {
	Get(storage.Key) (storage.Row, error)
	TxGet(int64, storage.Key) (storage.Row, error)
	TxSet(int64, storage.Key, storage.Row) error
	Begin() int64
	Commit(int64) error
	Rollback(int64) error
	TxGetForUpdate(txID int64, skipLocked bool, key storage.Key) (storage.Row, error)
	LockKeys(txID int64, skipLocked bool, keys []storage.Key) error
}

func scheduleVacuum(delay time.Duration, txTables storage.TxTables) {
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

func schedulePersister(delay time.Duration, txTables storage.TxTables, ssTables storage.ROTables) {
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
