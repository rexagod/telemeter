package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/openshift/telemeter/pkg/authorize"
	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/goleak"
)

const sampleMetrics = `
up{job="test",label="value0"} 1
up{job="test",label="value1"} 1
up{job="test",label="value2"} 0
`

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

func TestServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	var receiveServer *httptest.Server
	{
		// This is the receiveServer that the Telemeter Server is going to forward to
		// upon receiving metrics itself.
		receiveServer = httptest.NewServer(mockedReceiver(t))
		defer receiveServer.Close()
	}

	for _, tcase := range []struct {
		name      string
		extraOpts func(opts *Options)
	}{
		{
			name:      "without OIDC",
			extraOpts: func(opts *Options) {},
		},
		//{
		//	// TODO(bwplotka): Mock OIDC server and uncomment.
		//	name: "with OIDC",
		//	extraOpts: func(opts *Options) {
		//		opts.OIDCIssuer = "..."
		//		opts.OIDCClientID = "..."
		//		opts.OIDCClientSecret = "..."
		//		opts.OIDCAudienceEndpoint = "...api/v2/"
		//	},
		//},
	} {
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

			{
				opts := defaultOpts()
				opts.ForwardURL = receiveServer.URL
				opts.TenantID = "default-tenant"
				opts.Labels = map[string]string{"cluster": "test"}
				opts.clusterIDKey = "cluster"
				opts.Logger = log.NewLogfmtLogger(os.Stderr)
				opts.Whitelist = []string{"up"}
				opts.Ratelimit = 0
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
			}

			// TODO(bwplotka): Test failure cases!

			for _, cluster := range []string{"cluster1", "cluster2", "cluster3"} {
				t.Run(cluster, func(t *testing.T) {
					tokenResp := authorize.TokenResponse{}
					t.Run("authorize", func(t *testing.T) {
						// Authorize first.
						req, err := http.NewRequest(http.MethodPost, "http://"+ext.Addr().String()+"/authorize", nil)
						ok(t, err)

						q := req.URL.Query()
						q.Add("id", cluster)
						req.URL.RawQuery = q.Encode()
						req.Header.Set("Authorization", "bearer whatever")
						resp, err := http.DefaultClient.Do(req.WithContext(ctx))
						ok(t, err)

						defer resp.Body.Close()
						body, err := ioutil.ReadAll(resp.Body)
						ok(t, err)

						equals(t, 2, resp.StatusCode/100, "request did not return 2xx, but %s: %s", resp.Status, string(body))

						ok(t, json.Unmarshal(body, &tokenResp))
					})

					for i := 0; i < 5; i++ {
						t.Run("upload", func(t *testing.T) {
							metricFamilies := readMetrics(t, sampleMetrics, cluster)

							buf := &bytes.Buffer{}
							encoder := expfmt.NewEncoder(buf, expfmt.FmtProtoDelim)
							for _, f := range metricFamilies {
								ok(t, encoder.Encode(f))
							}

							req, err := http.NewRequest(http.MethodPost, "http://"+ext.Addr().String()+"/upload", buf)
							ok(t, err)

							req.Header.Set("Content-Type", string(expfmt.FmtProtoDelim))
							req.Header.Set("Authorization", "bearer "+tokenResp.Token)
							resp, err := http.DefaultClient.Do(req.WithContext(ctx))
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

func readMetrics(t *testing.T, m string, cluster string) []*clientmodel.MetricFamily {
	var families []*clientmodel.MetricFamily

	now := timestamp.FromTime(time.Now())
	decoder := expfmt.NewDecoder(bytes.NewBufferString(m), expfmt.FmtText)
	for {
		family := clientmodel.MetricFamily{}
		if err := decoder.Decode(&family); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		for _, m := range family.Metric {
			m.TimestampMs = &now
			k := "cluster"
			v := cluster
			m.Label = append(m.Label, &clientmodel.LabelPair{Name: &k, Value: &v})
		}
		families = append(families, &family)
	}
	return families
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

		tsc := len(wreq.Timeseries)
		if tsc != 3 {
			t.Errorf("expected 3 timeseries to be forwarded, got %d", tsc)
		}

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
