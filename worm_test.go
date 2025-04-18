package wormdb_test

import (
	"bytes"
	"fmt"
	"log"
	"os"

	bwdb "github.com/pschou/go-wormdb"
)

var (
	index [][]byte
)

func init() {
	f, err := os.Create("test.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := bwdb.NewBinarySearch()
	db, err := bwdb.New(f,
		bwdb.WithSearch(bs),
	)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		db.Add([]byte(fmt.Sprintf("aaaaaaaaaaaaaa%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}
	for i := 0; i < 100; i++ {
		db.Add([]byte(fmt.Sprintf("b hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}
	for i := 0; i < 100; i++ {
		db.Add([]byte(fmt.Sprintf("c hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}
	db.Add([]byte("hello world"))
	db.Add([]byte("hello world abc00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world def00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world ghi00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world jkl00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world mno00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	for i := 0; i < 100; i++ {
		db.Add([]byte(fmt.Sprintf("hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}
	db.Add([]byte("hello world qrs00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	db.Add([]byte("hello world tuv00000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	//db.Finalize() // Optional as it is called on close below
	db.Close()
	index = bs.Index
	ExampleNewDiskBinarySearch()
}

func ExampleNew() {
	f, err := os.Create("new.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := bwdb.NewBinarySearch()
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
	err = db.Get([]byte("hello world ab"), func(rec []byte) error {
		fmt.Println("found:", string(rec))
		return nil
	})
	// Output:
	// found: hello world abc
}

func ExampleWalk() {
	f, err := os.Create("walk.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := bwdb.NewBinarySearch()
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
	// Making the file larger so it spans blocks
	//for i := 4; i < 100; i++ {
	//	db.Add([]byte(fmt.Sprintf("hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	//}

	db.Finalize()
	i := 0
	walker := db.NewWalker()
	for walker.Scan() {
		rec := walker.Bytes()
		fmt.Println("step", i, string(rec))
		i++
	}
	// Output:
	// step 0 hello world
	// step 1 hello world abc
	// step 2 hello world def
	// step 3 hello world ghi
}

func ExampleNewDiskBinarySearch() {
	f, err := os.Create("disk_data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ind, err := os.Create("disk_index.db")
	if err != nil {
		log.Fatal(err)
	}
	defer ind.Close()

	bs := bwdb.NewDiskBinarySearch(ind)
	db, err := bwdb.New(f,
		bwdb.WithSearch(bs))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Add([]byte("hello world"))
	// Making the file larger
	for i := 4; i < 1000; i++ {
		db.Add([]byte(fmt.Sprintf("hello world a%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}
	db.Add([]byte("hello world abc"))
	db.Add([]byte("hello world def"))
	db.Add([]byte("hello world ghi"))
	// Making the file larger
	for i := 4; i < 1000; i++ {
		db.Add([]byte(fmt.Sprintf("hello world p%08d00000000000000000000000000000000000000000000000000000000000000000000000000000000", i)))
	}

	// Flush the database to disk and switch to read mode
	err = db.Finalize()
	if err != nil {
		log.Fatal(err)
	}

	// Load indexes into memory from disk
	err = bs.LoadIndexToMemory()
	if err != nil {
		log.Fatal(err)
	}

	err = db.Get([]byte("hello world de"), func(rec []byte) error {
		fmt.Println("found:", string(rec))
		return nil
	})

	// Output:
	// found: hello world def
}

func ExampleLoadDiskBinarySearch() {
	f, err := os.Open("disk_data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ind, err := os.Open("disk_index.db")
	if err != nil {
		log.Fatal(err)
	}
	defer ind.Close()

	bs, err := bwdb.LoadDiskBinarySearch(ind)
	if err != nil {
		log.Fatal(err)
	}

	db, err := bwdb.Open(f,
		bwdb.WithSearch(bs))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Get([]byte("hello world de"), func(rec []byte) error {
		fmt.Println("found:", string(rec))
		return nil
	})
	// Output:
	// found: hello world def
}

func ExampleOpen() {
	f, err := os.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	bs := bwdb.LoadBinarySearch(index)
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

func ExampleNewMerge() {
	f1, err := os.Create("new_merged.db")
	if err != nil {
		log.Fatal(err)
	}
	bs1 := bwdb.NewBinarySearch()
	db1, err := bwdb.New(f1,
		bwdb.WithSearch(bs1))
	if err != nil {
		log.Fatal(err)
	}
	defer db1.Close()
	db1.Add([]byte("hello world"))
	db1.Add([]byte("hello world aabc"))
	db1.Add([]byte("hello world cdef"))
	db1.Add([]byte("hello world ghi"))
	db1.Finalize()

	f2, err := os.Create("new_merged2.db")
	if err != nil {
		log.Fatal(err)
	}

	bs2 := bwdb.NewBinarySearch()
	db2, err := bwdb.New(f2,
		bwdb.WithSearch(bs2),
		bwdb.WithMerge(db1, bytes.Compare))
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()
	db2.Add([]byte("hello world"))
	db2.Add([]byte("hello world abc"))
	db2.Add([]byte("hello world def"))
	db2.Add([]byte("hello world ghi"))

	db2.Finalize()

	i := 0
	walker := db2.NewWalker()
	for walker.Scan() {
		rec := walker.Bytes()
		fmt.Println("step", i, string(rec))
		i++
	}

	// Output:
	// step 0 hello world
	// step 1 hello world aabc
	// step 2 hello world abc
	// step 3 hello world cdef
	// step 4 hello world def
	// step 5 hello world ghi
}
