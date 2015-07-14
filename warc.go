// package warc provides primitives for reading WARC files.
package warc

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

// Mode defines the way Reader will generate Records.
type Mode int

func (m Mode) String() string {
	switch m {
	case SequentialMode:
		return "SequentialMode"
	case AsynchronousMode:
		return "AsynchronousMode"
	}
	return ""
}

const (
	// SequentialMode means Records have to be consumed one by one and a call to
	// ReadRecord() invalidates the previous record. The benefit is that
	// Records have almost no overhead since they wrap around
	// the underlying Reader.
	SequentialMode Mode = iota
	// AsynchronousMode means calls to ReadRecord don't effect previously
	// returned Records. This mode copies the Record's content into
	// separate memory, thus bears memory overhead.
	AsynchronousMode
)

// Reader reads WARC records from WARC files.
type Reader struct {
	Mode Mode

	// Unexported fields.
	source io.ReadCloser
	reader *bufio.Reader
	record *Record
}

// Record represents a WARC record.
type Record struct {
	Header  map[string]string
	Content io.Reader
	Length  int
}

const (
	compressionNone = iota
	compressionBZIP
	compressionGZIP
)

// guessCompression returns the compression type of a data stream by matching
// the first two bytes with the magic numbers of compression formats.
func guessCompression(b *bufio.Reader) (int, error) {
	magic, err := b.Peek(2)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return compressionNone, err
	}
	switch {
	case magic[0] == 0x42 && magic[1] == 0x5a:
		return compressionBZIP, nil
	case magic[0] == 0x1f && magic[1] == 0x8b:
		return compressionGZIP, nil
	}
	return compressionNone, nil
}

// decompress automatically decompresses data streams and makes sure the result
// obeys the io.ReadCloser interface. This way callers don't need to check
// whether the underlying reader has a Close() function or not, they just call
// defer Close() on the result.
func decompress(r io.Reader) (res io.ReadCloser, err error) {
	// Create a buffered reader to peek the stream's magic number.
	dataReader := bufio.NewReader(r)
	compr, err := guessCompression(dataReader)
	if err != nil {
		return nil, err
	}
	switch compr {
	case compressionGZIP:
		gzipReader, err := gzip.NewReader(dataReader)
		if err != nil {
			return nil, err
		}
		res = gzipReader
	case compressionBZIP:
		bzipReader := bzip2.NewReader(dataReader)
		res = ioutil.NopCloser(bzipReader)
	case compressionNone:
		res = ioutil.NopCloser(dataReader)
	}
	return res, err
}

// sliceReader returns a new io.Reader for the next n bytes in source.
// If clone is true, the n bytes will be fully read from source and the
// resulting io.Reader will have its own copy of the data. Calls to the
// result's Read() function won't change the state of source.
// If clone is false, no bytes will be consumed from source and the resulting
// io.Reader will wrap itself around source. Each call to the result's Read()
// function will change the state of source.
func sliceReader(source io.Reader, size int, clone bool) (io.Reader, error) {
	reader := io.LimitReader(source, int64(size))
	if !clone {
		return reader, nil
	}
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(content), nil
}

// splitKeyValue parses WARC record header fields.
func splitKeyValue(line string) (string, string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return strings.ToLower(parts[0]), strings.TrimSpace(parts[1])
}

// NewReader creates a new WARC reader.
func NewReader(reader io.Reader, mode Mode) (*Reader, error) {
	source, err := decompress(reader)
	if err != nil {
		return nil, err
	}
	return &Reader{
		Mode:   mode,
		source: source,
		reader: bufio.NewReader(source),
	}, nil
}

// Close closes the reader.
func (r *Reader) Close() {
	if r.source != nil {
		r.source.Close()
		r.source = nil
		r.reader = nil
		r.record = nil
	}
}

// readLine reads the next line in the opened WARC file.
func (r *Reader) readLine() (string, error) {
	data, isPrefix, err := r.reader.ReadLine()
	if err != nil {
		return "", err
	}
	// Line was too long for the buffer.
	// TODO: rather return an error in this case? This function
	// is only used on header fields and they shouldn't exceed the buffer size
	// or should they?
	if isPrefix {
		buffer := new(bytes.Buffer)
		buffer.Write(data)
		for isPrefix {
			data, isPrefix, err = r.reader.ReadLine()
			if err != nil {
				return "", err
			}
			buffer.Write(data)
		}
		return buffer.String(), nil
	}
	return string(data), nil
}

// ReadRecord reads the next record from the opened WARC file.
func (r *Reader) ReadRecord() (*Record, error) {
	// Go to the position of the next record in the file.
	r.seekRecord()
	// Skip the record version line.
	if _, err := r.readLine(); err != nil {
		return nil, err
	}
	// Parse the record header.
	header := make(map[string]string)
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			break
		}
		if key, value := splitKeyValue(line); key != "" {
			header[key] = value
		}
	}
	// Determine the content length and then retrieve the record content.
	length, err := strconv.Atoi(header["content-length"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse field Content-Length: %v", err)
	}
	content, err := sliceReader(r.reader, length, r.Mode == AsynchronousMode)
	if err != nil {
		return nil, err
	}
	r.record = &Record{
		Header:  header,
		Content: content,
		Length:  length,
	}
	return r.record, nil
}

// seekRecord moves the Reader to the position of the next WARC record
// in the opened WARC file.
func (r *Reader) seekRecord() error {
	// No record was read yet? The r.reader must be at a start of the file and
	// thus the start of a record.
	if r.record == nil {
		return nil
	}
	// If the mode is set to SequentialMode, the underlying r.reader might be
	// anywhere inside the active record's block - depending on how much the
	// user actually consumed. So we have to make sure all content gets skipped
	// here.
	if r.Mode == SequentialMode {
		buffer := make([]byte, 4096)
		for {
			n, err := r.record.Content.Read(buffer)
			if n == 0 || err != nil {
				break
			}
		}
	}
	// Set to nil so it's safe to call this function several times without
	// destroying stuff.
	r.record = nil
	for i := 0; i < 2; i++ {
		line, err := r.readLine()
		if err != nil {
			return err
		}
		if line != "" {
			return fmt.Errorf("expected empty line, got %q", line)
		}
	}
	return nil
}
