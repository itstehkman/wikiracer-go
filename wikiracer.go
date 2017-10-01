package main

import (
  "net/http"
  "encoding/json"
  "fmt"
  //"bytes"
  "io/ioutil"
  "sync"
  "time"
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

func visitTitle(title, continueParam, plcontinueParam string, sbm *SafeStringBoolMap) {
  if continueParam == "||" {
    fmt.Println("continued")
  }
  url := makeURL(title, continueParam, plcontinueParam)
  if sbm.seen(url) {
    fmt.Println(url)
    return
  }
  fmt.Println("curr", title)
  sbm.set(url)
  res, err := http.Get(url)
  println(res.StatusCode)
  if err != nil{
    println("error")
    println(err)
    return
  }
  if res.StatusCode != 200 {
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
      go visitTitle(childTitle, "", "", sbm)
    }
  }

  if mediaWikiResponse.Continue != nil {
    fmt.Println("yo", title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"])
    go visitTitle(title, mediaWikiResponse.Continue["continue"], mediaWikiResponse.Continue["plcontinue"], sbm)
  }
}

func main() {
  sbm := SafeStringBoolMap{strings: make(map[string]bool)}
  visitTitle("Albert Einstein", "||", "736|0|Action-angle_variables", &sbm)
  for {
    ch := time.After(10)
    <-ch
  }
}
