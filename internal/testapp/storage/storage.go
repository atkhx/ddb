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

var (
	ErrAccountNotFound       = errors.New("account not found")
	ErrAccountRowInvalid     = errors.New("account row invalid")
	ErrAccountHasNoMoney     = errors.New("account has no money")
	ErrSaveAccountToFailed   = errors.New("save account to failed")
	ErrSaveAccountFromFailed = errors.New("save account from failed")
)

func (s *storage) GetAccountForUpdate(tx TX, user string) (model.Account, error) {
	accountId := model.GetAccountID(user)
	accountRow, err := s.db.TxGetForUpdate(tx, accountId)

	if err != nil {
		return model.Account{}, err
	} else if accountRow == nil {
		return model.Account{}, ErrAccountNotFound
	}

	result, ok := accountRow.(model.Account)
	if !ok {
		return model.Account{}, ErrAccountRowInvalid
	}

	return result, nil
}

func (s *storage) SendMoney(fromUser, toUser string, amount int64) {
	tx := s.db.Begin()

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

	accountFrom, err := s.GetAccountForUpdate(tx, fromUser)
	if err != nil {
		return
	}

	if accountFrom.Amount < amount {
		err = ErrAccountHasNoMoney
		return
	}

	accountTo, err := s.GetAccountForUpdate(tx, toUser)
	if err != nil {
		return
	}

	accountTo.Amount += amount
	accountFrom.Amount -= amount

	if err = s.db.TxSet(tx, model.GetAccountID(toUser), accountTo); err != nil {
		err = ErrSaveAccountToFailed
		return
	}

	if err = s.db.TxSet(tx, model.GetAccountID(fromUser), accountFrom); err != nil {
		err = ErrSaveAccountFromFailed
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
			log.Println("invalid account type", fmt.Sprintf("%v", accountRow))
			return
		}
		//log.Println("account", fmt.Sprintf("%v", accountRow))

		amount += account.Amount
	}

	log.Println("amount   ", amount)
	log.Println("expected ", 1000*len(users))
}
