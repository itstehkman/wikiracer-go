package main

import (
  "net/http"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "sync"
  "time"
  "golang.org/x/time/rate"
  "runtime"
  "os"
)

type SafeStringBoolMap struct {
  strings map[string]bool
  mut sync.Mutex
}

func (sbm *SafeStringBoolMap) set(title string) {
  sbm.mut.Lock()
  defer sbm.mut.Unlock()
  sbm.strings[title] = true
}

func (sbm *SafeStringBoolMap) seen(title string) bool {
  sbm.mut.Lock()
  defer sbm.mut.Unlock()
  return sbm.strings[title]
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

func makeRequest(title, continueParam, plcontinueParam string) *http.Request {
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

  req := makeRequest(title, continueParam, plcontinueParam)
  str := req.URL.String()
  if sbm.seen(str) {
    return
  }
  requestCh <- req
}

func visitTitle(req *http.Request, sbm *SafeStringBoolMap, requestCh chan <- *http.Request,
  endTitle string, rateLimiter *rate.Limiter) {

  title := req.URL.Query().Get("titles")
  continueParam := req.URL.Query().Get("continue")
  plcontinueParam := req.URL.Query().Get("plcontinue")

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
    limit := rateLimiter.Limit()
    if limit > 6 {
      rateLimiter.SetLimit(rateLimiter.Limit() - 5)
    }
    enqueueRequest(title, continueParam, plcontinueParam, sbm, requestCh)
    return
  }
  println(res.StatusCode)

  retryStatusCodes := map[int]bool{403: true, 429: true, 502: true}

  if _, ok := retryStatusCodes[res.StatusCode]; ok {
    fmt.Printf("Got status code %d, should retry...\n", res.StatusCode)
    limit := rateLimiter.Limit()
    if limit > 6 {
      rateLimiter.SetLimit(rateLimiter.Limit() - 5)
    }
    enqueueRequest(title, continueParam, plcontinueParam, sbm, requestCh)
    return
  } else if res.StatusCode != 200 {
    fmt.Println("Got status code ", res.StatusCode)
    return
  }

  rateLimiter.SetLimit(rateLimiter.Limit() + 1)

  sbm.set(url)

  defer res.Body.Close()
  body, err := ioutil.ReadAll(res.Body)
  var mediaWikiResponse MediaWikiResponse
  err = json.Unmarshal(body, &mediaWikiResponse)
  if err != nil {
    fmt.Println("unmarshall failed: ", err)
  }

  childrenTitles := mediaWikiResponse.childrenTitles()
  for _, childTitle := range childrenTitles {
    if childTitle != "" {
      go enqueueRequest(childTitle, "", "", sbm, requestCh)
    }
  }

  if mediaWikiResponse.Continue != nil {
    go enqueueRequest(title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"], sbm, requestCh)
  }
}

func processRequestQueue(requestCh chan *http.Request, sbm *SafeStringBoolMap, endTitle string) {
  rateLimiter := rate.NewLimiter(100, 50)
  for {
    fmt.Println(len(requestCh), len(sbm.strings), rateLimiter.Limit())
    req := <-requestCh
    reservation := rateLimiter.Reserve()
    time.Sleep(reservation.Delay())
    go visitTitle(req, sbm, requestCh, endTitle, rateLimiter)
  }
}

func main() {
  runtime.GOMAXPROCS(2)

  startTitle := "Albert Einstein"
  endTitle := "Pumpkin Pie"
  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  requestCh := make(chan *http.Request, 1024)
  enqueueRequest(startTitle, "", "", &sbm, requestCh)
  processRequestQueue(requestCh, &sbm, endTitle)

}
