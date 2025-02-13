package main

import (
	"fmt"
	"log"
	"strconv"

	"fib"
	"gateway/fabric"
)

func main() {
	forever := make(chan bool)

	err := fabric.ListenAndServe("fabric", "amqp://guest:guest@localhost:5672/", "fibonacci", "fabric.fib.*", handle)
	if err != nil {
		panic(err)
	}

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func handle(req *fabric.Request) *fabric.Response {
	in, err := strconv.Atoi(string(req.Body))
	if err != nil {
		h := map[string]string{"Status": "400"}
		res := &fabric.Response{Header: h, Body: []byte(err.Error()), Request: req}
		return res
	}
	fmt.Printf(" [.] fib(%d)\n", in)
	out := fib.Fibonacci(in)
	fmt.Printf(" [<] fib(%d) %d\n", in, out)

	resp := []byte(strconv.Itoa(out))
	res := &fabric.Response{Header: nil, Body: resp, Request: req}
	return res
}
