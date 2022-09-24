package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}


// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, vis visited, ch chan string, end chan bool) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		end <- true
		return
	}
	vis.mu.Lock()
	_, ok := vis.v[url]
	if ok {
		vis.mu.Unlock()
		end <- true
		return
	}
	body, urls, err := fetcher.Fetch(url)
	vis.v[url] = true
	vis.mu.Unlock()

	if err != nil {
		fmt.Println(err)
		end <- true
		return
	}
	ch <- fmt.Sprintf("found: %s %q\n", url, body)
	for _, u := range urls {
		go Crawl(u, depth-1, fetcher, vis, ch, end)
	}
	for i := 0; i < len(urls); i++ {
		<- end	
	}
	end <- true
	return
}

func main() {
	vis := visited{make(map[string]bool), sync.Mutex{}}
	// need a counter of how many values are pushed into the channel
	ch1 := make(chan string)
	ch2 := make(chan bool)
	go Crawl("https://golang.org/", 4, fetcher, vis, ch1, ch2)
	for{
		select {
		case s := <-ch1:
			fmt.Printf(s)
		case <- ch2:
			return
		}
	}
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult
// a map that count whether a url is visited
type visited struct {
	v map[string]bool
	mu sync.Mutex
}

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
