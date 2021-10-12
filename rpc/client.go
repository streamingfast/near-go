// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"reflect"
	"time"

	"github.com/streamingfast/logging"
	"github.com/ybbus/jsonrpc"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ClientOption = func(cli *Client) *Client

type Client struct {
	rpcURL    string
	rpcClient jsonrpc.RPCClient

	headers            http.Header
	requestIDGenerator func() int

	debug bool
}

func NewClient(rpcURL string, opts ...ClientOption) *Client {
	c := &Client{
		rpcURL: rpcURL,
		rpcClient: jsonrpc.NewClientWithOpts(rpcURL, &jsonrpc.RPCClientOpts{
			HTTPClient: &http.Client{
				Transport: &withLoggingRoundTripper{
					defaultLogger: &zlog,
					tracer:        tracer,
				}},
		}),
		requestIDGenerator: generateRequestID,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Client) GetBlock(ctx context.Context, blockId string) (out GetBlockByIDResult, err error) {
	block := map[string]string{
		"block_id": blockId,
	}

	err = c.callFor(ctx, &out, "block", block)
	return
}

func (c *Client) callFor(ctx context.Context, out interface{}, method string, params ...interface{}) error {
	req := jsonrpc.NewRequest(method, params...)
	req.ID = c.requestIDGenerator()

	var fields []zapcore.Field
	if tracer.Enabled() {
		fields = append(fields, zap.Reflect("params", params))
	}
	fields = append(fields, zapType("output", out))

	startTime := time.Now()
	decodingTime := time.Time{}

	logger := zlog.With(zap.Int("id", req.ID), zap.String("method", method))
	logger.Info("performing JSON-RPC call", fields...)
	defer func() {
		fields := []zapcore.Field{}
		if !decodingTime.IsZero() {
			fields = append(fields, zap.Duration("parsing", time.Since(decodingTime)))
		}
		fields = append(fields, zap.Duration("overall", time.Since(startTime)))

		logger.Info("performed JSON-RPC call", fields...)
	}()

	//TODO: https://github.com/ybbus/jsonrpc/pull/39/commits
	resp, err := c.rpcClient.CallRaw(req)
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return resp.Error
	}

	return resp.GetObject(out)
}

var requestCounter = atomic.NewInt64(0)

func generateRequestID() int {
	return int(requestCounter.Inc())
}

func zapType(key string, v interface{}) zap.Field {
	return zap.Stringer(key, zapTypeWrapper{v})
}

type zapTypeWrapper struct {
	v interface{}
}

func (w zapTypeWrapper) String() string {
	return reflect.TypeOf(w.v).String()
}

type withLoggingRoundTripper struct {
	defaultLogger **zap.Logger
	tracer        logging.Tracer
}

func (t *withLoggingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	logger := logging.Logger(request.Context(), *t.defaultLogger)

	debugEnabled := logger.Core().Enabled(zap.DebugLevel)
	traceEnabled := t.tracer.Enabled()

	if debugEnabled {
		requestDump, err := httputil.DumpRequestOut(request, true)
		if err != nil {
			panic(fmt.Errorf("unexpecting that httputil.DumpRequestOut would panic: %w", err))
		}

		logger.Debug("JSON-RPC request\n" + string(requestDump))
	}

	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	if debugEnabled {
		responseDump, err := httputil.DumpResponse(response, traceEnabled)
		if err != nil {
			panic(fmt.Errorf("unexpecting that httputil.DumpRequestOut would panic: %w", err))
		}

		logger.Debug("JSON-RPC response\n" + string(responseDump))
	}

	return response, nil
}
