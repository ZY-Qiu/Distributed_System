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
func MutexCrawl(url string, depth int, fetcher Fetcher, vis *visited) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	vis.mu.Lock()
	_, ok := vis.v[url]
	vis.v[url] = true
	vis.mu.Unlock()
	if ok {
		return
	}
	_, urls, err := fetcher.Fetch(url)

	if err != nil {
		return
	}

	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		go func(s string) {
			defer done.Done()
			MutexCrawl(s, depth-1, fetcher, vis)
		}(u)
	}
	done.Wait()
	return
}

func ChanCrawlWorker(url string, ch chan []string, fetcher Fetcher) {
	_, urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
	} else {
		ch <- urls
	}
}

func ChanCrawlMaster(depth int, fetcher Fetcher, ch chan []string) {
	vis := make(map[string]bool)
	// master listening on channel, and distributes new work to worker when hearing something
	n := 1
	for strlist := range ch {
		for _, str := range strlist {
			if v := vis[str]; v == false {
				vis[str] = true
				n += 1
				go ChanCrawlWorker(str, ch, fetcher)
			}
		}
		n -= 1
		if n == 0 {
			break
		}
	}
}

func main() {
	vis := visited{make(map[string]bool), sync.Mutex{}}
	// need a counter of how many values are pushed into the channel
	MutexCrawl("https://golang.org/", 4, fetcher, &vis)
	fmt.Printf("*****************\n")
	ch := make(chan []string)
	go func() { ch <- []string{"https://golang.org/"} }()
	ChanCrawlMaster(4, fetcher, ch)
}

// a map that count whether a url is visited
type visited struct {
	v  map[string]bool
	mu sync.Mutex
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		fmt.Printf("found: %s %q\n", url, res.body)
		return res.body, res.urls, nil
	}
	fmt.Printf("not found: %s\n", url)
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
