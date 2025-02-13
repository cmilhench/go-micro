package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"gateway/fabric"
)

var revision = "latest"

func main() {
	http.HandleFunc("/v", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, revision) })
	http.Handle("/", http.HandlerFunc(handler))
	http.ListenAndServe(":8080", nil)
}

// curl http://localhost:8080/ -XPOST -d '{"target":"fabric.fib.get", "detail":44}'
// curl http://localhost:8080/ -XPOST -d '{"target":"fabric.consignments.read"}'
// curl http://localhost:8080/ -XPOST -d '{"target":"fabric.consignments.create","detail":{"description": "", "weight": 9200}}'
// curl -u guest:guest http://localhost:15672/api/exchanges/%2F/fabric/bindings/source | jq '.'
func handler(w http.ResponseWriter, r *http.Request) {
	url := "amqp://guest:guest@localhost:5672/"
	exchange := "fabric"
	if r.Method != "POST" {
		return
	}
	if r.ContentLength == 0 {
		http.Error(w, "no payload", http.StatusBadRequest)
		return
	}
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	input := &struct {
		Version    string          `json:"version"`
		Target     string          `json:"target"`
		DetailType string          `json:"detail-type"`
		Detail     json.RawMessage `json:"detail"`
	}{}
	err = json.Unmarshal(buf, input)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, err := fabric.Publish(exchange, url, input.Target, "application/json", input.Detail)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Detect error payload and respond accordingly
	status, err := strconv.Atoi(res.Header["Status"])
	if err == nil {
		w.WriteHeader(status)
	}
	fmt.Fprintln(w, string(res.Body))
}
