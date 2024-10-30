# WORM-DB - Write Once Read Many Database

The WORMDB format is a binary database format designed for both efficient
storage and efficient lookups of sorted data.  The database works off a disk
binary file and an index file.  The index file provides the ability to lookup
which block a record should be located.  The record can then be retrieved and
returned.  A record in the WORMDB is always a binary slice of arbitrary length.
To search for an individual record, provide a unique prefix.

Note:  If the query prefix match matches multiple record indexs, either the
first record will be returned or an error.  Make sure your index is unique.

An example of how the database works:

If one is looking for the record starting with `abc123` they can use the Find()
function to find this record:

An example database (one entry per line)
```
abc122cat
abc123bat
abc124dob
```

```golang
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
```

Some benchmarking for query times:
```
$ go test -bench=.
goos: linux
goarch: amd64
pkg: github.com/pschou/go-wormdb
cpu: Intel(R) Xeon(R) CPU           X5650  @ 2.67GHz
BenchmarkSearchDriveCached-12             235136              4923 ns/op
BenchmarkSearch-12                        188329              6407 ns/op
PASS
```
