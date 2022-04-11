package model

import (
	"fmt"

	"github.com/atkhx/ddb/internal/keys"
)

func GetAccountID(user string) keys.StrKey {
	return keys.StrKey(fmt.Sprintf("account_%s", user))
}

type Account struct {
	ID     keys.StrKey
	Amount int64
}
