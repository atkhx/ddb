package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "user7:s$cret@tcp(127.0.0.1:3306)/testdb")
	if err != nil {
		log.Fatalln("open connection failed:", err)
	}

	defer db.Close()

	rows, err := db.Query("SELECT VERSION()")
	if err != nil {
		log.Fatalln("query rows failed:", err)
	}

	defer rows.Close()

	var server string
	var version string

	for rows.Next() {
		if err = rows.Scan(&server, &version); err != nil {
			log.Fatalln("query row failed:", err)
		}

		fmt.Println("scanned value:", server, version)
	}
}

func main2() {
	db, err := sql.Open("mysql", "user7:s$cret@tcp(127.0.0.1:3306)/testdb")
	if err != nil {
		log.Fatalln("open connection failed:", err)
	}

	defer db.Close()

	var version string

	row := db.QueryRow("SELECT VERSION()")
	fmt.Println("row:", row)
	fmt.Println("lets scan")
	err = row.Scan(&version)
	if err != nil {
		log.Fatalln("query row failed:", err)
	}

	fmt.Println(version)
}
