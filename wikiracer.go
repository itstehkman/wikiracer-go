package main

import (
  "net/http"
  "encoding/json"
  "fmt"
  //"bytes"
  "io/ioutil"
  "sync"
  "time"
  "golang.org/x/time/rate"
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
    fmt.Println(links)
    for _, linkmap := range links {
      lm := linkmap.(map[string]interface{})
      childrenTitles = append(childrenTitles, lm["title"].(string))
    }
  }
  return childrenTitles
}

func makeURL(title, continueParam, plcontinueParam string) string {
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
  return req.URL.String()
}

func enqueueRequest(title, continueParam, plcontinueParam string, sbm *SafeStringBoolMap, requestCh chan <- string) {
  url := makeURL(title, continueParam, plcontinueParam)
  if sbm.seen(url) {
    fmt.Println(url)
    return
  }
  fmt.Println("curr", title)
  sbm.set(url)

  requestCh <- url
}

func visitURL(url string, sbm *SafeStringBoolMap, requestCh chan <- string) {
  res, err := http.Get(url)
  println(res.StatusCode)
  if err != nil{
    fmt.Println("error")
    fmt.Println(err)
    return
  }

  retryStatusCodes := map[int]bool{403: true, 429: true, 502: true}

  if _, ok := retryStatusCodes[res.StatusCode]; ok {
    fmt.Printf("Got status code %d, should retry...\n", res.StatusCode)
    return
  } else if res.StatusCode != 200 {
    fmt.Println("Got status code ", res.StatusCode)
    return
  }

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
    fmt.Println("yo", title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"])
    go enqueueRequest(title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"], sbm, requestCh)
  }
}

func processRequestQueue(requestCh chan string, sbm *SafeStringBoolMap) {
  rateLimiter := rate.NewLimiter(1000, 50)
  for {
    url := <-requestCh
    rateLimiter.Allow()
    go visitURL(url, sbm, requestCh)
  }
}

func main() {
  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  requestCh := make(chan string, 1024)
  enqueueRequest("Albert Einstein", "", "", &sbm, requestCh)
  processRequestQueue(requestCh, &sbm)

  for {
    ch := time.After(10)
    <-ch
  }
}
