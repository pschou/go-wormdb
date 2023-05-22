package wormdb_test

import (
	"fmt"
	"log"
	"os"

	"github.com/pschou/go-wormdb"
)

func ExampleBlah() {
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

		//fmt.Printf("%+v\n", wdb)
		//wdb.Print()

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

		//wdb.Print()

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
