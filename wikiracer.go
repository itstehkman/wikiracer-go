package main

import (
  "net/http"
  _ "net/http/pprof"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "sync"
  "time"
  "golang.org/x/time/rate"
  "runtime"
  "os"
  "log"
)

type SafeStringBoolMap struct {
  strings map[string]bool
  mut sync.Mutex
}

func (sbm *SafeStringBoolMap) set(key string) {
  sbm.mut.Lock()
  defer sbm.mut.Unlock()
  sbm.strings[key] = true
}

func (sbm *SafeStringBoolMap) seen(key string) bool {
  sbm.mut.Lock()
  defer sbm.mut.Unlock()
  return sbm.strings[key]
}

type MediaWikiResponse struct {
  Query map[string](map[string](map[string]interface{}))
  Continue map[string]string
}

func (mwr *MediaWikiResponse) childrenTitles() []string {
  childrenTitles := make([]string, 1)
  pages := mwr.Query["pages"]
  for _,v := range pages {
    maybeLinks := v["links"]
    if maybeLinks == nil {
      continue
    }
    links := maybeLinks.([]interface{})
    //fmt.Println(links)
    for _, linkmap := range links {
      lm := linkmap.(map[string]interface{})
      childrenTitles = append(childrenTitles, lm["title"].(string))
    }
  }
  return childrenTitles
}

type Page struct {
  title string
  childrenTitles []string
  lock sync.RWMutex
  donePopulating bool
}

func (p *Page) AddChildrenTitles(titles []string) {
  p.lock.Lock()
  defer p.lock.Unlock()
  p.childrenTitles = append(p.childrenTitles, titles...)
}

func (p *Page) ChildrenTitles() []string{
  p.lock.RLock()
  defer p.lock.RUnlock()
  return p.childrenTitles
}

func (p *Page) PopulateChildrenTitles() {
  //enqueueRequest(p.title, "", "", sbm)
  /*
    req := createRequest(p.Title, "", "")
    requester.Request(req, func(res *http.Response, err error) {

    })

   */
}

type RequestWithResponseChan struct {
  req         *http.Request
  resCallback func(*http.Response, error)
}

type Requester struct {
  rateLimiter *SafeRateLimiter
  // tuples of request, response channel
  requestCh chan *RequestWithResponseChan
}

func NewRequester(rateLimit, burst, chanSize, queueProcessors int) *Requester {
  requester := &Requester{
    rateLimiter: &SafeRateLimiter{rateLimiter: rate.NewLimiter(rateLimit, burst)},
    requestCh: make(chan *RequestWithResponseChan, chanSize),
  }

  // Process the queue of requests while respecting the rate limiter
  for i := 0; i < queueProcessors; i++ {
    go func(requester *Requester){
      for {
        reqWithResCallback := <-requester.requestCh
        reservation := requester.rateLimiter.ReserveN(time.Now(), 1)
        time.Sleep(reservation)
        res, err := http.Get(reqWithResCallback.req.URL.String())
        reqWithResCallback.resCallback(res, err)
      }
    }(requester)
  }

  return requester
}

func (requester *Requester) Request(req *http.Request, resCallback func(*http.Response, error)) {
  requester.requestCh <- &RequestWithResponseChan{req: req, resCallback: resCallback}
}

type SafeRateLimiter struct {
  rateLimiter *rate.Limiter
  lock sync.RWMutex
}

func (limiter *SafeRateLimiter) Limit() rate.Limit {
  limiter.lock.RLock()
  defer limiter.lock.RUnlock()
  return limiter.rateLimiter.Limit()
}

func (limiter *SafeRateLimiter) ReserveN(t time.Time, n int) *rate.Reservation {
  limiter.lock.Lock()
  defer limiter.lock.Unlock()
  return limiter.rateLimiter.ReserveN(t, n)
}

func createRequest(title, continueParam, plcontinueParam string) *http.Request {
  req, _ := http.NewRequest("GET", "http://en.wikipedia.org/w/api.php", nil)
  q := req.URL.Query()
  q.Add("action", "query")
  q.Add("format", "json")
  q.Add("prop", "links")
  q.Add("titles", title)
  if continueParam != "" {
    q.Add("continue", continueParam)
  }
  if plcontinueParam != "" {
    q.Add("plcontinue", plcontinueParam)
  }
  req.URL.RawQuery = q.Encode()
  return req
}

func enqueueRequest(title, continueParam, plcontinueParam string, sbm *SafeStringBoolMap,
  requestCh chan <- *http.Request) {

  req := createRequest(title, continueParam, plcontinueParam)
  str := req.URL.String()
  //fmt.Println(str)
  if sbm.seen(str) {
    return
  }
  requestCh <- req
}

func visitTitle(req *http.Request, sbm *SafeStringBoolMap, requestCh chan <- *http.Request,
  endTitle string, rateLimiter *SafeRateLimiter) {

  title := req.URL.Query().Get("titles")
  continueParam := req.URL.Query().Get("continue")
  plcontinueParam := req.URL.Query().Get("plcontinue")

  //fmt.Println(title)
  if title == endTitle {
    fmt.Printf("Reached title %s !! :)\n", endTitle)
    os.Exit(0)
    return
  }

  url := req.URL.String()
  res, err := http.Get(url)
  if err != nil{
    fmt.Println("error")
    fmt.Println(err)
    enqueueRequest(title, continueParam, plcontinueParam, sbm, requestCh)
    return
  }
  //println(res.StatusCode)

  retryStatusCodes := map[int]bool{403: true, 429: true, 502: true}

  if _, ok := retryStatusCodes[res.StatusCode]; ok {
    fmt.Printf("Got status code %d, should retry...\n", res.StatusCode)
    enqueueRequest(title, continueParam, plcontinueParam, sbm, requestCh)
    return
  } else if res.StatusCode != 200 {
    fmt.Println("Got status code ", res.StatusCode)
    return
  }

  sbm.set(url)

  defer res.Body.Close()
  body, err := ioutil.ReadAll(res.Body)
  var mediaWikiResponse MediaWikiResponse
  err = json.Unmarshal(body, &mediaWikiResponse)
  if err != nil {
    fmt.Println("unmarshall failed: ", err)
  }

  childrenTitles := mediaWikiResponse.childrenTitles()
  //fmt.Println(childrenTitles)
  //reservation := rateLimiter.ReserveN(time.Now(), 5)
  //time.Sleep(reservation.Delay())
  for _, childTitle := range childrenTitles {
    if childTitle != "" {
      go enqueueRequest(childTitle, "", "", sbm, requestCh)
    }
  }

  if mediaWikiResponse.Continue != nil {
    go enqueueRequest(title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"], sbm, requestCh)
  }
}

func processRequestQueue(requestCh chan *http.Request, sbm *SafeStringBoolMap, endTitle string,
  rateLimiter *SafeRateLimiter) {

  for {
    fmt.Println(len(requestCh), len(sbm.strings), rateLimiter.Limit())
    i := 0
    requests := make([]*http.Request, 0, 50)
    for ; i < 50; i++ {
      //fmt.Println("a", i)
      req := <-requestCh
      //fmt.Printf("here %p", req)
      requests = append(requests, req)
      //fmt.Println("b", i, requests[i])
      //fmt.Println("c", len(requestCh))
      if len(requestCh) == 0 {
        break
      }
    }
    //fmt.Println("d", len(requests))
    reservation := rateLimiter.ReserveN(time.Now(), i - 1)
    time.Sleep(reservation.Delay())
    for i := 0; i < len(requests); i++ {
      //fmt.Printf("e %p\n", requests[i])
      go visitTitle(requests[i], sbm, requestCh, endTitle, rateLimiter)
    }
  }
}

func main() {
  go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
  }()

  runtime.GOMAXPROCS(2)
  startTitle := "Football"
  endTitle := "Team Sports"
  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  requestCh := make(chan *http.Request, 10000)
  enqueueRequest(startTitle, "", "", &sbm, requestCh)
  safeRateLimiter := SafeRateLimiter{rateLimiter: rate.NewLimiter(100, 50)}
  for i := 0; i < 3; i++ {
    go processRequestQueue(requestCh, &sbm, endTitle, &safeRateLimiter)
  }

  for {
    time.Sleep(1000)
  }

  startPage := &Page{title: startTitle, childrenTitles: make([]string, 1)}
  startPage.PopulateChildrenTitles()
  for _, childTitle := range startPage.ChildrenTitles() {
    fmt.Println(childTitle)
  }

}