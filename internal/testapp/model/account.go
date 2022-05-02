package model

import (
	"fmt"

	"github.com/atkhx/ddb/pkg/base"
)

func GetAccountID(user string) base.StrKey {
	return base.StrKey(fmt.Sprintf("account_%s", user))
}

type Account struct {
	ID     base.StrKey
	Amount int64
}
