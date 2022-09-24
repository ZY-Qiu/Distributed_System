package main

import (
	"io"
	"os"
	"strings"
)

type rot13Reader struct {
	r io.Reader
}

func (rot rot13Reader) Read(buf []byte) (int, error) {
	length, err := rot.r.Read(buf)
	if err != nil {
		return length, err
	}
	// rotate by 13 in alphabet
	for i := 0; i < length; i++ {
		v := buf[i]
		if v >= 'a' && v <= 'z' {
			buf[i] = 'a' + (v - 'a' + 13) % 26
			//fmt.Println('a' + (v - 'a' + 13) % 26)
		}
		if v >= 'A' && v <= 'Z' {
			buf[i] = 'A' + (v - 'A' + 13) % 26
			//fmt.Println('A' + (v - 'A' + 13) % 26)
		}
	}
	return length, nil
}

func main() {
	s := strings.NewReader("Lbh penpxrq gur pbqr!")
	r := rot13Reader{s}
	io.Copy(os.Stdout, &r)
}
