package main

import (
	"database/sql"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

func DBWriter(db *sql.DB, bufferSize int, data chan *FSNodeStat, end chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	buffer := make([]*FSNodeStat, bufferSize)
	count := 0
	ending := false
	for {
		select {
		case d := <-data:
			buffer[count] = d
			count++
			if count == bufferSize {
				BatchInsertData(buffer[:count], db)
				count = 0
			}
		case <-end:
			ending = true
		default:
			if ending {
				BatchInsertData(buffer[:count], db)
				return
			}
		}
	}
}

func ConnectDB(path string) (*sql.DB, error) {
	var db *sql.DB

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		log.Printf("Creating new database: %s\n", path)
		db, err = sql.Open("sqlite3", path)
		if err != nil {
			log.Fatal(err)
		}
		_, err = db.Exec(`CREATE TABLE FileNodes (
			Id INTEGER PRIMARY KEY,
			ParentId INTEGER,
			Path TEXT,
			IsDir BOOLEAN,
			Size INTEGER,
			Count INTEGER,
			SFileCount INTEGER,
			SFileSize INTEGER,
			MFileCount INTEGER,
			MFileSize INTEGER,
			LFileCount INTEGER,
			LFileSize INTEGER,
			XLFileCount INTEGER,
			XLFileSize INTEGER,
			XXLFileCount INTEGER,
			XXLFileSize INTEGER
		)`)
		if err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatalf("Error when checking file: %v\n", err)
	} else {
		log.Fatalf("%s is already present\n", path)
	}

	return db, nil
}

func CloseDB(db *sql.DB) {
	defer db.Close()
}

func BatchInsertData(data []*FSNodeStat, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(`INSERT INTO 
	FileNodes(
		Id, 
		ParentId, 
		Path, 
		IsDir, 
		Size, 
		Count, 
		SFileCount, 
		SFileSize, 
		MFileCount, 
		MFileSize, 
		LFileCount, 
		LFileSize, 
		XLFileCount, 
		XLFileSize, 
		XXLFileCount, 
		XXLFileSize
	) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, datum := range data {
		_, err := stmt.Exec(
			datum.Id,
			datum.ParentId,
			datum.Path,
			datum.IsDir,
			datum.Size,
			datum.Count,
			datum.SFileCount,
			datum.SFileSize,
			datum.MFileCount,
			datum.MFileSize,
			datum.LFileCount,
			datum.LFileSize,
			datum.XLFileCount,
			datum.XLFileSize,
			datum.XXLFileCount,
			datum.XXLFileSize,
		)
		if err != nil {
			tx.Rollback() // roll back if failed
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
