package main

import "golang.org/x/tour/reader"

type MyReader struct{}

// TODO: Add a Read([]byte) (int, error) method to MyReader.
func (mr MyReader) Read(buf []byte) (int, error) {
	for i := 0; i < len(buf); i++ {
		buf[i] = 'A'
	}
	return len(buf), nil
}

func main() {
	reader.Validate(MyReader{})
}
