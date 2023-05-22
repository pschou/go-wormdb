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
rec, _ := wdb.Find("abc123")
fmt.Println(string(rec))   // Prints: abc123bat
```

Thus a search for `abc123` will get `abc123bat` as the reply.

Likewise if one searches for `abc123cat` no records will be returned.
