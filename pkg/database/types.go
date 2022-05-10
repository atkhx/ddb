package database

import (
	"fmt"
)

type DbRequest interface{}

type DbResult struct {
	Error       *DbError
	ExecResult  *ExecResult
	FetchResult *FetchResult
}

type DbError struct {
	Code    int64
	Message string
	Origin  error
}

func (e DbError) Error() string {
	if e.Origin != nil {
		return fmt.Sprintf("[%d] - %s: %s", e.Code, e.Message, e.Origin.Error())
	}

	return fmt.Sprintf("[%d] - %s", e.Code, e.Message)
}

type ExecResult struct {
	RowsAffected int64
	LastInsertId int64
}

type FetchResult struct {
	Cols []Column
	Rows [][]string
}

type Column struct {
	Catalog    string
	Schema     string
	Table      string
	TableAlias string

	Name string
}
