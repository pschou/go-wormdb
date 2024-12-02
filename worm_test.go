package wormdb_test

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
	bs := &bwdb.SearchBinary{}
	db, err := bwdb.New(f,
		bwdb.WithSearch(bs),
	)
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
	index = bs.Index
}

func ExampleNew() {
	f, err := os.Create("new.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := &bwdb.SearchBinary{Index: index}
	db, err := bwdb.New(f,
		bwdb.WithSearch(bs))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Add([]byte("hello world"))
	db.Add([]byte("hello world abc"))
	db.Add([]byte("hello world def"))
	db.Add([]byte("hello world ghi"))

	db.Finalize()
	db.Get([]byte("hello world ab"), func(rec []byte) error {
		fmt.Println(string(rec))
		return nil
	})
	// Output:
	// hello world abc
}

func ExampleOpen() {
	f, err := os.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := &bwdb.SearchBinary{Index: index}
	db, err := bwdb.Open(f,
		bwdb.WithSearch(bs))
	if err != nil {
		log.Fatal(err)
	}
	// Note that the index must be stored out of band
	db.Get([]byte("hello world qrs"), func(rec []byte) error {
		fmt.Printf("rec: %q err: %v\n", rec, err)
		return nil
	})
	db.Close()
	// Output:
	// rec: "hello world qrs00000000000000000000000000000000000000000000000000000000000000000000000000000000" err: <nil>
}
