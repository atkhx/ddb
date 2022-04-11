package storage

import (
	"fmt"
	"log"

	"github.com/atkhx/ddb/internal/testapp/model"
	"github.com/pkg/errors"
)

func New(db DB) *storage {
	return &storage{db: db}
}

type storage struct {
	db DB
}

func (s *storage) GiveFirstAmount(users []string) {
	var err error
	for _, user := range users {
		accountId := model.GetAccountID(user)
		account := model.Account{ID: accountId, Amount: 1000}

		if err = s.db.Set(accountId, account); err != nil {
			err = errors.Wrap(err, "give money for user failed")
			return
		}
	}
}

func (s *storage) GetAccountForUpdate(tx TX, user string) (model.Account, error) {
	accountId := model.GetAccountID(user)
	accountRow, err := s.db.TxGetForUpdate(tx, accountId)

	if err != nil {
		return model.Account{}, errors.Wrap(err, fmt.Sprintf("get account failed %d %s", tx, accountId))
	} else if accountRow == nil {
		return model.Account{}, errors.New("account not found " + accountId.String())
	}

	result, ok := accountRow.(model.Account)
	if !ok {
		return model.Account{}, errors.New("account type invalid")
	}

	return result, nil
}

func (s *storage) SendMoney(fromUser, toUser string, amount int64) {
	tx := s.db.Begin()
	//tx := db.Begin(storage.ReadCommitted())
	//tx := db.Begin(storage.RepeatableRead())
	var err error
	defer func() {
		if err != nil {
			if err := s.db.Rollback(tx); err != nil {
				log.Println("rollback transaction", tx, err)
			}
		} else {
			if err := s.db.Commit(tx); err != nil {
				log.Println("commit transaction failed", err)
			}
		}
	}()

	//if err = db.LockKeys(tx, []internal.Key{getAccountId(fromUser), getAccountId(toUser)}); err != nil {
	//	return
	//}

	accountFrom, err := s.GetAccountForUpdate(tx, fromUser)
	if err != nil {
		//err = errors.Wrap(err, "account FROM not found")
		return
	}

	if accountFrom.Amount < amount {
		err = errors.New("account FROM has no money")
		return
	}

	accountTo, err := s.GetAccountForUpdate(tx, toUser)
	if err != nil {
		//err = errors.Wrap(err, "account TO not found")
		return
	}

	accountTo.Amount += amount
	accountFrom.Amount -= amount

	if err = s.db.TxSet(tx, model.GetAccountID(toUser), accountTo); err != nil {
		err = errors.Wrap(err, "save account TO failed")
		return
	}

	if err = s.db.TxSet(tx, model.GetAccountID(fromUser), accountFrom); err != nil {
		err = errors.Wrap(err, "save account FROM failed")
		return
	}
}

func (s *storage) CheckTotalAmount(users []string) {
	var amount int64
	for _, user := range users {
		accountRow, err := s.db.Get(model.GetAccountID(user))
		if err != nil {
			log.Println("get account failed", err)
			return
		}

		account, ok := accountRow.(model.Account)
		if !ok {
			log.Println("invalid account type")
			return
		}

		amount += account.Amount
	}

	log.Println("amount   ", amount)
	log.Println("expected ", 1000*len(users))
}
