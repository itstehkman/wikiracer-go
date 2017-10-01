package main

import (
  "net/http"
  "encoding/json"
  "fmt"
  //"bytes"
  "io/ioutil"
)

type MediaWikiResponse struct {
  Query map[string](map[string](map[string]interface{}))
  Continue map[string]string
}

func (mwr *MediaWikiResponse) childrenTitles() []string {
  childrenTitles := make([]string, 1)
  pages := mwr.Query["pages"]
  for _,v := range pages {
    links := v["links"].([]interface{})
    fmt.Println(links)
    for _,linkmap := range links {
      lm := linkmap.(map[string]interface{})
      childrenTitles = append(childrenTitles, lm["title"].(string))
    }
  }
  return childrenTitles
}

func visitTitle(title, continueParam, plcontinueParam string) {
  childrenTitles := make([]string, 1)
  fmt.Println("curr", title)

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

  println(req.URL.String())

  res, err := http.Get(req.URL.String())
  println(res.StatusCode)
  if err != nil {
    println("error")
    println(err)
    return
  }
  defer res.Body.Close()
  body, err := ioutil.ReadAll(res.Body)
  var topLevelMap map[string]interface{}
  var mediaWikiResponse MediaWikiResponse
  err = json.Unmarshal(body, &topLevelMap)
  fmt.Println(topLevelMap)
  err = json.Unmarshal(body, &mediaWikiResponse)
  fmt.Println(mediaWikiResponse)
  if err != nil {
    fmt.Println("unmarshall failed: ", err)
  }
  //fmt.Println(topLevelMap)

  pages := topLevelMap["query"].(map[string]interface{})["pages"].(map[string]interface{})
  //fmt.Println(pages)
  for _,v := range pages {
    m := v.(map[string]interface{})
    links := m["links"].([]interface{})
    //fmt.Println(links)
    for _,linkmap := range links {
      lm := linkmap.(map[string]interface{})
      childrenTitles = append(childrenTitles, lm["title"].(string))
    }
  }
  fmt.Println(childrenTitles)
  for _, childTitle := range childrenTitles {
    if childTitle != "" {
      //visitTitle(childTitle, "", "")
    }
  }
}

func main() {
  visitTitle("Albert Einstein", "", "")
}
