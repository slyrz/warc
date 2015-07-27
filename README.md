# warc

warc provides primitives for reading and writing [WARC files](http://bibnum.bnf.fr/WARC/)
in Go. This version is based on [edsu's warc library](https://github.com/edsu/warc),
but many changes were made:

This package works with WARC files in plain text, GZip compression and BZip2 compression out of the box.
The record content is exposed via `io.Reader` interfaces. Types and functions were renamed
to follow [Go's naming conventions](https://blog.golang.org/package-names).
All external dependencies were removed. A Writer was added.

### Example

The following example reads a WARC file from `stdin` and prints informations
about each record to `stdout`.

```go
// Read WARC file from os.Stdin.
reader, err := warc.NewReader(os.Stdin)
if err != nil {
	panic(err)
}
defer reader.Close()
// Iterate over records.
for {
	record, err := reader.ReadRecord()
	if err != nil {
		break
	}
	fmt.Println("Record:")
	for key, value := range record.Header {
		fmt.Printf("\t%v = %v\n", key, value)
	}
}
```

The next example writes a single WARC record to `stdout`.

```go
// Write WARC records to os.Stdout.
writer := warc.NewWriter(os.Stdout)
// Create a new WARC record.
record := warc.NewRecord()
// Store metadata in the header. Key should be all lowercase.
record.Header["warc-type"] = "resource"
record.Header["content-type"] = "plain/text"
// Assign the content to the record.
record.Content = strings.NewReader("Hello, World!")
// Write the record to os.Stdout.
if _, err := writer.WriteRecord(record); err != nil {
	panic(err)
}
```

### Performance

Parsing WARC files is as fast as it can get. The real overhead stems from
the underlying compression algorithms. So if you are about to parse the same
file several times for whatever reason, consider decompressing it first.

### License

warc is released under CC0 license.
You can find a copy of the CC0 License in the [LICENSE](./LICENSE) file.
