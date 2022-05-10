package main

import (
	"context"
	"log"

	server2 "github.com/atkhx/ddb/pkg/database"
	"github.com/atkhx/ddb/pkg/database/server"
	"github.com/atkhx/ddb/pkg/database/server/handler"
	"github.com/atkhx/ddb/pkg/database/server/protocol/mysql"
)

var err error

func main() {
	defer func() {
		if err != nil {
			log.Fatal(err)
		}
	}()

	ctx := context.Background()
	//ctx, _ := context.WithTimeout(ctx, time.Second*15)

	srv := server.NewServer(
		handler.NewHandler(
			mysql.NewProtocol(),
			&processor{},
		),
	)
	err = srv.Listen(ctx)
}

type processor struct {
}

func (p processor) Execute(request server2.DbRequest) (server2.DbResult, error) {
	if request == "SELECT VERSION()" {
		return server2.DbResult{
			FetchResult: &server2.FetchResult{
				Cols: []server2.Column{
					{
						Schema:     "schema",
						Table:      "table",
						TableAlias: "table-alias",
						Name:       "server",
					},
					{
						Schema:     "schema",
						Table:      "table",
						TableAlias: "table-alias",
						Name:       "version",
					},
				},
				Rows: [][]string{
					{
						"ddb 1",
						"v1.0",
					},
					{
						"some long field content",
						"v1.1",
					},
				},
			},
		}, nil
	}

	if request == "select @@version_comment limit 1" {
		return server2.DbResult{
			FetchResult: &server2.FetchResult{
				Cols: []server2.Column{
					{
						Schema:     "schema",
						Table:      "table",
						TableAlias: "table-alias",
						Name:       "version",
					},
				},
				Rows: [][]string{
					{
						"ddb v1.0",
					},
				},
			},
		}, nil
	}

	return server2.DbResult{}, nil
}
