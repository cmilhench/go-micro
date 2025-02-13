package handler

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"consignments"
	"consignments/model"
	"gateway/fabric"
)

func HandleRPC(svc *consignments.Service) func(req *fabric.Request) *fabric.Response {
	read := getConsignments(svc)
	create := createConsignment(svc)
	return func(req *fabric.Request) *fabric.Response {
		switch {
		case strings.HasSuffix(req.RoutingKey, ".consignments.create"):
			return create(req)
		case strings.HasSuffix(req.RoutingKey, ".consignments.read"):
			return read(req)
		default:
			// 404
			h := map[string]string{"Status": "404"}
			res := &fabric.Response{Header: h, Body: []byte("not found"), Request: req}
			return res
		}
	}
}

func getConsignments(svc *consignments.Service) func(req *fabric.Request) *fabric.Response {
	return func(req *fabric.Request) *fabric.Response {
		log.Printf(" [.] GetConsignments(%s)", string(req.Body))
		resp, err := svc.GetConsignments(context.Background())
		if err != nil {
			log.Printf("failed to read consignment: %v\n", err)
			h := map[string]string{"Status": "400"}
			res := &fabric.Response{Header: h, Body: []byte(err.Error()), Request: req}
			return res
		}
		out, err := json.Marshal(resp)
		if err != nil {
			log.Printf("failed to marshal json: %v\n", err)
			h := map[string]string{"Status": "400"}
			res := &fabric.Response{Header: h, Body: []byte(err.Error()), Request: req}
			return res
		}
		res := &fabric.Response{Header: nil, Body: out, Request: req}
		return res
	}
}

func createConsignment(svc *consignments.Service) func(*fabric.Request) *fabric.Response {
	return func(req *fabric.Request) *fabric.Response {
		log.Printf(" [.] CreateConsignment(%s)", string(req.Body))
		payload := &model.Consignment{}
		err := json.Unmarshal(req.Body, payload)
		if err != nil {
			log.Printf("failed to unmarshal json: %v\n", err)
			h := map[string]string{"Status": "400"}
			res := &fabric.Response{Header: h, Body: []byte(err.Error()), Request: req}
			return res
		}
		resp, err := svc.CreateConsignment(context.Background(), payload)
		if err != nil {
			log.Printf("failed to create consignment: %v\n", err)
			h := map[string]string{"Status": "400"}
			res := &fabric.Response{Header: h, Body: []byte(err.Error()), Request: req}
			return res
		}
		out, err := json.Marshal(resp)
		if err != nil {
			log.Printf("failed to marshal json: %v\n", err)
			res := &fabric.Response{Header: nil, Body: []byte(err.Error()), Request: req}
			return res
		}
		res := &fabric.Response{Header: nil, Body: out, Request: req}
		return res
	}
}
