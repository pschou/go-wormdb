package wormdb_test

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/pschou/go-wormdb"
)

func ExampleNew() {
	// Create a new wormdb
	fh, _ := os.Create("uuid.wdb")
	defer fh.Close()
	wdb := wormdb.New(fh)

	// Load a file with uuid values and suffixes
	in, _ := os.Open("uuid_input.dat")
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		err := wdb.Add([]byte(scanner.Text()))
		if err != nil {
			log.Panic(err)
		}
	}
	// Finalize the load and commit to disk caches.
	wdb.Finalize()

	// Save off the index for future reloading
	idx, _ := os.Create("uuid.idx")
	wdb.SaveIndex(idx)
	idx.Close()

	// Do a search for how fast the load happens
	toFind := "ec83ca32-1e9e-4b6c-8cf5-8e28535630e3."
	fmt.Println("Looking for:", toFind)
	find, _ := wdb.Find([]byte(toFind))
	fmt.Println("Found      :", string(find))
	// Output:
	// Looking for: ec83ca32-1e9e-4b6c-8cf5-8e28535630e3.
	// Found      : ec83ca32-1e9e-4b6c-8cf5-8e28535630e3.176
}

func BenchmarkSearchDriveCached(b *testing.B) {
	fh, _ := os.Open("uuid.idx")
	defer fh.Close()
	idx, _ := os.Open("uuid.idx")
	defer idx.Close()
	wdb, _ := wormdb.Load(fh, idx)
	toFind := []byte("ec83ca32-1e9e-4b6c-")
	for n := 0; n < b.N; n++ {
		wdb.Find(toFind)
	}
}

func BenchmarkSearch(b *testing.B) {
	fh, _ := os.Open("uuid.idx")
	defer fh.Close()
	idx, _ := os.Open("uuid.idx")
	defer idx.Close()
	wdb, _ := wormdb.Load(fh, idx)
	for n := 0; n < b.N; n++ {
		wdb.Find([]byte(fmt.Sprintf("%02x%02x", n, n)))
	}
}

func ExampleNewAndLoad() {
	fh, _ := os.Create("data.wdb")
	{ // Create a new wormdb
		wdb := wormdb.New(fh)
		for i := 0; i < 4000; i++ {
			err := wdb.Add([]byte(fmt.Sprintf("blah%05dabc", i)))
			if err != nil {
				log.Panic(err)
			}
		}
		wdb.Finalize()

		// Save off the index for loading
		idx, _ := os.Create("data.idx")
		wdb.SaveIndex(idx)
		idx.Close()

		for _, finders := range []string{"blah00123", "blah01234", "blah12345"} {
			find, err := wdb.Find([]byte(finders))
			fmt.Println("find", finders, "at start:", string(find), err)
		}
	}

	{ // Load an existing wormdb
		idx, _ := os.Open("data.idx")
		wdb, err := wormdb.Load(fh, idx)
		if err != nil {
			log.Panic(err)
		}
		idx.Close()

		for _, finders := range []string{"blah00123", "blah01234", "blah12345"} {
			find, err := wdb.Find([]byte(finders))
			fmt.Println("find", finders, "at start:", string(find), err)
		}
	}
	// Output:
	// find blah00123 at start: blah00123abc <nil>
	// find blah01234 at start: blah01234abc <nil>
	// find blah12345 at start:  <nil>
	// find blah00123 at start: blah00123abc <nil>
	// find blah01234 at start: blah01234abc <nil>
	// find blah12345 at start:  <nil>
}
