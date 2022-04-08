package main

import (
	"fmt"
	"log"

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
	txTables := storage.NewTxManager(storage.NewReadCommitted(), txFactory, rwTabFactory)

	db := storage.NewStorage(ssTables, txTables, txLocks)
	giveFirstAmount(db)

	tx2 := db.Begin()
	tx1 := db.Begin()

	userId1 := getAccountId("user_1")
	userId2 := getAccountId("user_2")

	if err := db.Lock(tx2, false, []storage.Key{userId1, userId2}); err != nil {
		log.Fatalln(err)
	}

	if err := db.TxSet(tx2, userId1, account{amount: 1200}); err != nil {
		err = errors.Wrap(err, "save account TO failed")
		log.Fatalln(err)
	}

	if err := db.TxSet(tx2, userId2, account{amount: 800}); err != nil {
		err = errors.Wrap(err, "save account TO failed")
		log.Fatalln(err)
	}

	if err := db.Commit(tx2); err != nil {
		log.Fatalln(err)
	}

	log.Println(db.TxGet(tx1, userId1))
	log.Println(db.TxGet(tx1, userId2))

	db.Commit(tx1)

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
	//accountRow, err := db.TxGetForUpdate(tx, true, accountId)
	accountRow, err := db.TxGet(tx, accountId)
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

	if err = db.LockKeys(tx, false, []storage.Key{fromUser, toUser}); err != nil {
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
	Set(key storage.Key, row storage.Row) error
	TxGet(int64, storage.Key) (storage.Row, error)
	TxSet(int64, storage.Key, storage.Row) error
	Begin() int64
	Commit(int64) error
	Rollback(int64) error
	TxGetForUpdate(txID int64, skipLocked bool, key storage.Key) (storage.Row, error)
	LockKeys(txID int64, skipLocked bool, keys []storage.Key) error
}
