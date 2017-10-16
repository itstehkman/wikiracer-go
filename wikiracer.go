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
  "strings"
)

type Wikiracer struct {
  startTitle string
  endTitle string
  requester *Requester
  sbm *SafeStringBoolMap
}

func NewWikiracer(startTitle, endTitle string, requester *Requester,
  sbm *SafeStringBoolMap) *Wikiracer {

  return &Wikiracer{startTitle: startTitle, endTitle: endTitle, requester: requester, sbm: sbm}
}

func (racer *Wikiracer) Race() {
  startPage := &Page{title: racer.startTitle, childrenPages: make([]*Page, 1)}
  pageCh := make(chan *Page, 10000)
  pageCh <- startPage

  fmt.Println(startPage)
  for i := 0; i < 5; i++ {
    go racer.ProcessPageQueue(pageCh)
  }
}

func (racer *Wikiracer) ProcessPageQueue(pageCh chan *Page) {
  for p := range pageCh {
    go racer.ProcessPage(p, pageCh)
  }
}

func (racer *Wikiracer) ProcessPage(p *Page, pageCh chan *Page) {
  racer.PopulateChildrenPages(p)
  fmt.Println(len(pageCh), len(racer.sbm.strings), racer.requester.rateLimiter.Limit())
  for _, childPage := range p.ChildrenPages() {
    if childPage != nil && !racer.sbm.seen(childPage.title) {
      pageCh <- childPage
    }
  }
}

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

type ErrorShouldRetry struct {
  s string
}

func (err *ErrorShouldRetry) Error() string {
  return err.s
}

type Page struct {
  title string
  childrenPages []*Page
  lock sync.RWMutex
  donePopulating bool
}

func (p *Page) AddChildrenTitles(titles []string) {
  p.lock.Lock()
  defer p.lock.Unlock()
  for _, title := range titles {
    if title != "" {
      childPage := &Page{title: title, childrenPages: make([]*Page, 1)}
      p.childrenPages = append(p.childrenPages, childPage)
    }
  }
}

func (p *Page) ChildrenPages() []*Page {
  p.lock.RLock()
  defer p.lock.RUnlock()
  return p.childrenPages
}

func (racer *Wikiracer) PopulateChildrenPages(p *Page) {
  continueParam := ""
  plcontinueParam := ""

  for !p.donePopulating {
    done := make(chan struct{})
    req := createRequest(p.title, continueParam, plcontinueParam)

    racer.requester.Request(req, done, func(res *http.Response, err error) {
      continueParamCandidate, plcontinueParamCandidate, err := racer.handleMediaWikiResponse(p, continueParam,
        plcontinueParam, res, err)
      if err == nil {
        continueParam = continueParamCandidate
        plcontinueParam = plcontinueParamCandidate
        p.donePopulating = continueParam == ""
      }
    })

    <-done
  }
}

type RequestWithResponseChan struct {
  req         *http.Request
  resCallback func(*http.Response, error)
  // done struct if request is to be done sync wrt caller
  done chan <- struct{}
}

type Requester struct {
  rateLimiter *SafeRateLimiter
  // tuples of request, response channel
  requestCh chan *RequestWithResponseChan
}

func NewRequester(rateLimit rate.Limit, burst, chanSize, queueProcessors int) *Requester {
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
        time.Sleep(reservation.Delay())
        res, err := http.Get(reqWithResCallback.req.URL.String())
        reqWithResCallback.resCallback(res, err)
        if reqWithResCallback.done != nil {
          reqWithResCallback.done <- struct{}{}
        }
      }
    }(requester)
  }

  return requester
}

func (requester *Requester) Request(req *http.Request, done chan <- struct{},
  resCallback func(*http.Response, error)) {

  requester.requestCh <- &RequestWithResponseChan{req: req, resCallback: resCallback, done: done}
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

func (racer *Wikiracer) handleMediaWikiResponse(p *Page, continueParam, plcontinueParam string,
  res *http.Response, err error) (string, string, error) {

  if strings.Contains(strings.ToLower(p.title), strings.ToLower(racer.endTitle)) {
    fmt.Printf("Reached title %s !! :)\n", racer.endTitle)
    os.Exit(0)
  }

  retryStatusCodes := map[int]bool{403: true, 429: true, 502: true}

  if err != nil{
    fmt.Println("error")
    fmt.Println(err)
    return "", "", err
  } else if _, ok := retryStatusCodes[res.StatusCode]; ok {
    fmt.Printf("Got status code %d, should retry...\n", res.StatusCode)
    return "", "", fmt.Errorf("Got status code %d, should retry...", res.StatusCode)
  } else if res.StatusCode != 200 {
    fmt.Println("Got status code ", res.StatusCode)
    return "", "", fmt.Errorf("Got status code %d", res.StatusCode)
  }

  racer.sbm.set(p.title)

  defer res.Body.Close()
  body, err := ioutil.ReadAll(res.Body)
  var mediaWikiResponse MediaWikiResponse
  err = json.Unmarshal(body, &mediaWikiResponse)
  if err != nil {
    fmt.Println("unmarshall failed: ", err)
  }

  childrenTitles := mediaWikiResponse.childrenTitles()
  p.AddChildrenTitles(childrenTitles)

  if mediaWikiResponse.Continue != nil {
    return mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"], nil
  } else {
    return "", "", nil
  }
}

func main() {
  go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
  }()

  runtime.GOMAXPROCS(2)

  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  requester := NewRequester(100, 50, 10000, 3)
  racer := NewWikiracer("Football", "Team Sports", requester, &sbm)
  racer.Race()

  for {
    time.Sleep(1000)
  }
}