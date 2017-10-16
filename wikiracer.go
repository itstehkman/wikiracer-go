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
  "flag"
  "github.com/oleiade/lane"
)

type Wikiracer struct {
  startTitle string
  endTitle string
  requester *GetRequester
  sbm *SafeStringBoolMap
  queueProcessors int
}

func NewWikiracer(startTitle, endTitle string, requester *GetRequester,
  sbm *SafeStringBoolMap, queueProcessors int) *Wikiracer {

  return &Wikiracer{startTitle: startTitle, endTitle: endTitle, requester: requester, sbm: sbm,
    queueProcessors: queueProcessors}
}

// Creates a request with the start title, and issues a mediawiki request from there.
// That media wiki request will make several other requests, which will be processed by the
// Requester's queue
func (racer *Wikiracer) Race(resultCh chan []string) {
  startPage := NewPage(racer.startTitle, nil, 0)
  req := racer.createRequest(racer.startTitle, "", "")
  racer.IssueMediaWikiRequest(req, startPage, "", "", resultCh)
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
    for _, linkmap := range links {
      lm := linkmap.(map[string]interface{})
      childrenTitles = append(childrenTitles, lm["title"].(string))
    }
  }
  return childrenTitles
}

type Page struct {
  title string
  parent *Page
  lock sync.RWMutex
  depth int
}

func NewPage(title string, parent *Page, depth int) *Page {
  return &Page{title: title, parent: parent, depth: depth}
}


type RequestWithResponseChan struct {
  req         *http.Request
  resCallback func(*http.Response, error)
}

// A requester will isue get requests
type GetRequester struct {
  rateLimiter *SafeRateLimiter
  // priority queue of RequestWithResponseChan objects
  requestPQ *lane.PQueue
}

// Creates a Requester, which is used to make HTTP Get requests at some rate limit.
// It handles its own internal priority queue for getting requests
func NewRequester(rateLimit rate.Limit, burst, queueProcessors int) *GetRequester {
  requester := &GetRequester{
    rateLimiter: &SafeRateLimiter{rateLimiter: rate.NewLimiter(rateLimit, burst)},
    requestPQ: lane.NewPQueue(lane.MINPQ),
  }

  // Process the queue of requests while respecting the rate limiter
  for i := 0; i < queueProcessors; i++ {
    go func(requester *GetRequester){
      for {
        requests := make([]*RequestWithResponseChan, 0)
        numRequests := 0
        for numRequests < burst && requester.requestPQ.Size() != 0 {
          reqWithResCallbackInterface, _ := requester.requestPQ.Pop()
          if reqWithResCallbackInterface != nil {
            reqWithResCallback := reqWithResCallbackInterface.(*RequestWithResponseChan)
            requests = append(requests, reqWithResCallback)
            numRequests++
          }
        }
        reservation := requester.rateLimiter.ReserveN(time.Now(), numRequests)
        time.Sleep(reservation.Delay())
        for i := 0; i < numRequests; i++ {
          reqWithResCallback := requests[i]
          res, err := http.Get(reqWithResCallback.req.URL.String())
          reqWithResCallback.resCallback(res, err)
        }
      }
    }(requester)
  }

  return requester
}

// Push a request and its callback onto the priority queue
func (requester *GetRequester) Request(req *http.Request, priority int, resCallback func(*http.Response, error)) {
  reqWithResChan := &RequestWithResponseChan{req: req, resCallback: resCallback}
  requester.requestPQ.Push(reqWithResChan, priority)
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

// Given a title, continueParam, and plcontinueParam (specific keys to the media wiki request)
// return a usable *http.Request object with those params.
func (racer *Wikiracer) createRequest(title, continueParam, plcontinueParam string) *http.Request {
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

// Creates a request, with retry logic. Will also make requests for the children media wiki responses
// One very important factor to this algorithm is the priority of the request, which is the depth
// of the link in our BFS. We want to prioritize short links and not deep links, which could get us
// too far down the rabbit hole and wasting precious time
func (racer *Wikiracer) IssueMediaWikiRequest(req *http.Request, p *Page, continueParam,
  plcontinueParam string, resultCh chan []string) {

  if strings.ToLower(p.title) == strings.ToLower(racer.endTitle) {
    fmt.Printf("Reached title %s !! :)\n", racer.endTitle)

    path := make([]string, 0)
    currPage := p
    for currPage != nil {
      path = append(path, currPage.title)
      currPage = currPage.parent
    }
    for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
      path[i], path[j] = path[j], path[i]
    }
    resultCh <- path
    return
  }

  racer.requester.Request(req, p.depth, func(res *http.Response, err error) {
    retryStatusCodes := map[int]bool{403: true, 429: true, 502: true}
    if err != nil{
      fmt.Println("error")
      fmt.Println(err)
      racer.IssueMediaWikiRequest(req, p, continueParam, plcontinueParam, resultCh)
      return
    } else if _, ok := retryStatusCodes[res.StatusCode]; ok {
      fmt.Printf("Got status code %d, should retry...\n", res.StatusCode)
      racer.IssueMediaWikiRequest(req, p, continueParam, plcontinueParam, resultCh)
      return
    } else if res.StatusCode != 200 {
      fmt.Println("Got status code ", res.StatusCode)
      racer.IssueMediaWikiRequest(req, p, continueParam, plcontinueParam, resultCh)
      return
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
    for _, childTitle := range childrenTitles {
      req := racer.createRequest(childTitle, "", "")
      childPage := NewPage(childTitle, p, p.depth + 1)
      go racer.IssueMediaWikiRequest(req, childPage, "", "", resultCh)
    }

    if mediaWikiResponse.Continue != nil {
      req := racer.createRequest(p.title, mediaWikiResponse.Continue["continue"],
        mediaWikiResponse.Continue["plcontinue"])
      go racer.IssueMediaWikiRequest(req, p, mediaWikiResponse.Continue["continue"],
        mediaWikiResponse.Continue["plcontinue"], resultCh)
    }
  })
}

func main() {
  // Allows for live profiling via http://localhost:6060/debug/pprof. Use go tool pprof <url>
  go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
  }()

  startTitle := flag.String("start", "", "The title to start at ")
  endTitle := flag.String("end", "", "The title to end at ")
  numProc := flag.Int("num_proc", runtime.NumCPU(), "Number of processors to use")
  mediaWikiRateLimit := flag.Float64("rate_limit", 60, "Rate limit to use on the media wiki api")
  mediaWikiBurst := flag.Int("burst", 50, "Burst to use for the media wiki api")
  flag.Parse()

  runtime.GOMAXPROCS(*numProc)

  resultCh := make(chan []string)
  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  requester := NewRequester(rate.Limit(*mediaWikiRateLimit), *mediaWikiBurst, 5000)
  racer := NewWikiracer(*startTitle, *endTitle, requester, &sbm, 10)
  racer.Race(resultCh)

  // Wait for the result
  for {
    select {
    case path := <-resultCh:
      fmt.Printf("The path to get from %s to %s is\n", *startTitle, *endTitle)
      fmt.Println(path)
      os.Exit(0)
      return
    case <-time.After(1 * time.Second):
    }
  }
}