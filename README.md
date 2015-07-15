# warc

warc provides primitives for reading [WARC files](http://bibnum.bnf.fr/WARC/)
in Go. This version is based on [edsu's warc library](https://github.com/edsu/warc),
but many changes were made:

This package works with WARC files in plain text, GZip compression and BZip2 compression out of the box.
The record content is exposed via `io.Reader` interfaces. Types and functions were renamed
to follow [Go's naming conventions](https://blog.golang.org/package-names).
All external dependencies were removed.

### Example

The following example reads a WARC file from `stdin` and prints informations
about each record to `stdout`.

```go
package main

import (
	"fmt"
	"github.com/slyrz/warc"
	"os"
)

func main() {
	reader, err := warc.NewReader(os.Stdin, warc.SequentialMode)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}
		fmt.Printf("Record of size %d:\n", record.Length)
		for key, value := range record.Header {
			fmt.Printf("\t%v = %v\n", key, value)
		}
	}
}
```

### Performance

Parsing WARC files is as fast as it can get. The real overhead stems from
the underlying compression algorithms. So if you are about to parse the same
file several times for whatever reason, consider decompressing it first.

### License

warc is released under CC0 license.
You can find a copy of the CC0 License in the [LICENSE](./LICENSE) file.
