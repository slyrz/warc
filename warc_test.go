package warc_test

import (
	"crypto/sha1"
	"fmt"
	"github.com/slyrz/warc"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func testFileHash(t *testing.T, path string, mode warc.Mode) {
	t.Logf("testFileHash %q, mode %v", path, mode)

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := warc.NewReaderMode(file, mode)
	if err != nil {
		t.Fatalf("warc.NewReaderMode failed for %q: %v", path, err)
	}
	defer reader.Close()

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("failed to read all record content: %v", err)
			}
			break
		}
		content, err := ioutil.ReadAll(record.Content)
		if err != nil {
			t.Fatalf("failed to read all record content: %v", err)
		}
		hash := fmt.Sprintf("sha1:%x", sha1.Sum(content))
		if hash != record.Header["warc-block-digest"] {
			t.Fatalf("expected %q, got %q", record.Header["warc-block-digest"], hash)
		}
	}
}

func testFileScan(t *testing.T, path string, mode warc.Mode) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := warc.NewReaderMode(file, mode)
	if err != nil {
		t.Fatalf("warc.NewReaderMode failed for %q: %v", path, err)
	}
	defer reader.Close()

	total := 0
	for {
		if _, err := reader.ReadRecord(); err != nil {
			break
		}
		total++
	}
	if total != 50 {
		t.Fatalf("expected 50 records, got %v", total)
	}
}

func TestReader(t *testing.T) {
	var paths = []string{
		"testdata/test.warc",
		"testdata/test.warc.gz",
		"testdata/test.warc.bz2",
	}
	for _, path := range paths {
		testFileHash(t, path, warc.SequentialMode)
		testFileHash(t, path, warc.AsynchronousMode)
		testFileScan(t, path, warc.SequentialMode)
		testFileScan(t, path, warc.AsynchronousMode)
	}
}

func ExampleReader() {
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
		fmt.Printf("Record of size %d:\n", record.Length)
		for key, value := range record.Header {
			fmt.Printf("\t%v = %v\n", key, value)
		}
	}
}

func ExampleWriter() {
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
}
