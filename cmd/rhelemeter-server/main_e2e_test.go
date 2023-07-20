package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/goleak"
)

var expectedTimeSeries = []prompb.TimeSeries{
	{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "cluster", Value: "dynamic"},
			{Name: "job", Value: "test"},
			{Name: "label", Value: "value0"},
		},
		Samples: []prompb.Sample{{Value: 1}},
	},
	{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "cluster", Value: "dynamic"},
			{Name: "job", Value: "test"},
			{Name: "label", Value: "value1"},
		},
		Samples: []prompb.Sample{{Value: 1}},
	},
	{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "cluster", Value: "dynamic"},
			{Name: "job", Value: "test"},
			{Name: "label", Value: "value2"},
		},
		Samples: []prompb.Sample{{Value: 0}},
	},
}

// ok fails the test if an err is not nil.
func ok(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("\033[31munexpected error: %v\033[39m\n", err)
	}
}

func formatMessage(msgAndArgs []interface{}) string {
	if len(msgAndArgs) == 0 {
		return ""
	}

	if msg, ok := msgAndArgs[0].(string); ok {
		return fmt.Sprintf("\n\nmsg: "+msg, msgAndArgs[1:]...)
	}
	return ""
}

// Equals fails the test if exp is not equal to act.
func equals(t *testing.T, exp, act interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(exp, act) {
		t.Fatalf("\033[31m%s\n\nexp: %#v\n\ngot: %#v\033[39m\n", formatMessage(msgAndArgs), exp, act)
	}
}

func TestServerRHEL(t *testing.T) {
	defer goleak.VerifyNone(t)

	receiveServer := httptest.NewServer(mockedReceiver(t))
	defer receiveServer.Close()

	telemeterClient, err := makeMTLSClient()
	ok(t, err)

	testCases := []struct {
		name      string
		extraOpts func(opts *Options)
	}{
		{
			name: "mTLS",
			extraOpts: func(opts *Options) {
				opts.TLSKeyPath = "testdata/server-private-key.pem"
				opts.TLSCertificatePath = "testdata/server-cert.pem"
				opts.TLSCACertificatePath = "testdata/ca-cert.pem"
			},
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			prometheus.DefaultRegisterer = prometheus.NewRegistry()

			ext, err := net.Listen("tcp", "127.0.0.1:0")
			ok(t, err)

			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer func() {
				cancel()
				wg.Wait()
			}()

			opts := setTestDefaultOpts()
			opts.ForwardURL = receiveServer.URL
			tcase.extraOpts(opts)

			local, err := net.Listen("tcp", "127.0.0.1:0")
			ok(t, err)

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := opts.Run(ctx, ext, local); err != context.Canceled {
					t.Error(err)
					return
				}
			}()

			// Wait for server to start by pinging it.
			for i := 0; i < 30; i++ {
				time.Sleep(100 * time.Millisecond)

				res, err := telemeterClient.Get("https://" + ext.Addr().String() + "/")
				if err != nil {
					fmt.Println("Waiting for server to start...", err)
					continue
				}

				res.Body.Close()

				if res.StatusCode == http.StatusOK {
					break
				}
				fmt.Println("Waiting for server to start...", res.StatusCode)
			}

			for _, cluster := range []string{"cluster1"} {
				t.Run(cluster, func(t *testing.T) {

					for i := 0; i < 1; i++ {
						t.Run("upload", func(t *testing.T) {
							var wr prompb.WriteRequest
							wr.Timeseries = expectedTimeSeries
							data, err := proto.Marshal(&wr)
							ok(t, err)

							compressedData := snappy.Encode(nil, data)

							req, err := http.NewRequest(http.MethodPost, "https://"+ext.Addr().String()+"/metrics/v1/receive", bytes.NewReader(compressedData))
							ok(t, err)

							req.Header.Set("Content-Type", string(expfmt.FmtProtoDelim))
							resp, err := telemeterClient.Do(req.WithContext(ctx))
							ok(t, err)

							defer resp.Body.Close()

							body, err := ioutil.ReadAll(resp.Body)
							ok(t, err)

							equals(t, http.StatusOK, resp.StatusCode, string(body))
						})
					}
				})
			}
		})
	}
}

func setTestDefaultOpts() *Options {
	opts := defaultOpts()

	opts.Labels = map[string]string{"cluster": "test"}
	opts.Logger = log.NewLogfmtLogger(os.Stderr)
	opts.Whitelist = []string{"up"}
	opts.Ratelimit = 0
	return opts
}

func makeMTLSClient() (*http.Client, error) {
	cert, err := tls.LoadX509KeyPair("testdata/client-cert.pem", "testdata/client-private-key.pem")
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile("testdata/ca-cert.pem")
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("failed to add ca cert")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	return &http.Client{Transport: transport}, nil
}

// mockedReceiver unmarshalls the request body into prompb.WriteRequests
// and asserts the seeing contents against the pre-defined expectedTimeSeries from the top.
func mockedReceiver(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed reading body from forward request: %v", err)
		}

		reqBuf, err := snappy.Decode(nil, body)
		if err != nil {
			t.Errorf("failed to decode the snappy request: %v", err)
		}

		var wreq prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
			t.Errorf("failed to unmarshal WriteRequest: %v", err)
		}

		equals(t, len(expectedTimeSeries), len(wreq.Timeseries))

		for i, ts := range expectedTimeSeries {
			for j, l := range ts.Labels {
				wl := wreq.Timeseries[i].Labels[j]
				if l.Name != wl.Name {
					t.Errorf("expected label name %s, got %s", l.Name, wl.Name)
				}
				if l.Value == "dynamic" {
					continue
				}
				if l.Value != wl.Value {
					t.Errorf("expected label value %s, got %s", l.Value, wl.Value)
				}
			}
			for j, s := range ts.Samples {
				ws := wreq.Timeseries[i].Samples[j]
				if s.Value != ws.Value {
					t.Errorf("expected value for sample %2.f, got %2.f", s.Value, ws.Value)
				}
			}
		}
	}
}
