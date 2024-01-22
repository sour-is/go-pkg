package main

import (
	"database/sql"
	"log"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "./test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`drop table if exists foo`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table foo (bar jsonb)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`insert into foo (bar) values ('["one"]')`)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`select j.value from foo, json_each(bar) j `)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			log.Fatal(err)
		}
	
		log.Println("GOT: ", s)
	}
}