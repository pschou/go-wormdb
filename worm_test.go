package bwdb_test

import (
	"fmt"
	"log"
	"os"

	bwdb "github.com/pschou/go-wormdb"
)

var index [][]byte

func init() {
	f, err := os.Create("test.db")
	if err != nil {
		log.Fatal(err)
	}
	db, err := bwdb.New(f, 0, 4096)
	if err != nil {
		log.Fatal(err)
	}
	db.Add([]byte("hello world"))
	db.Add([]byte("hello world abc00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world def00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world ghi00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world jkl00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world mno00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	for i := 0; i < 100; i++ {
		db.Add([]byte(fmt.Sprintf("hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
		//fmt.Printf("adding:  hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000\n", i)
	}
	db.Add([]byte("hello world qrs00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world tuv00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Close()
	index = db.Index
}

func ExampleNew() {
	f, err := os.Create("new.db")
	if err != nil {
		log.Fatal(err)
	}
	db, err := bwdb.New(f, 0, 4096)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Add([]byte("hello world"))
	db.Add([]byte("hello world abc"))
	db.Add([]byte("hello world def"))
	db.Add([]byte("hello world ghi"))

	db.Finalize()
	rec, _ := db.Get([]byte("hello world ab"))
	fmt.Println(string(rec))
	// Output:
	// hello world abc
}

func ExampleOpen() {
	f, err := os.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	db, err := bwdb.Open(f, 0, 4096)
	if err != nil {
		log.Fatal(err)
	}
	// Note that the index must be stored out of band
	db.Index = index
	rec, err := db.Get([]byte("hello world qrs"))
	fmt.Printf("rec: %q err: %v\n", rec, err)
	db.Close()
	// Output:
	// rec: "hello world qrs00000000000000000000000000000000000000000000000000000000000000000000000000000000" err: <nil>
}
