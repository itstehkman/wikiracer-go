package main

import (
  "net/http"
  "encoding/json"
  "fmt"
  "bytes"
  "io/ioutil"
)

func main() {
  values := map[string]string{"action": "query", "format": "json", "prop": "links", "title": "Albert+Einstein"}
  jsonValues, err := json.Marshal(values)
  if err != nil {
    fmt.Println(err)
    return
  }
  res, err := http.Post("http://www.wikipedia.org/w/api.php", "application/json", bytes.NewBuffer(jsonValues))
  println(res.StatusCode)
  if err != nil {
    println("error")
    println(err)
    return
  }
  defer res.Body.Close()
  body, err := ioutil.ReadAll(res.Body)
  var topLevelMap map[string]interface{}

  err = json.Unmarshal(body, &topLevelMap)
  println(topLevelMap)
}
