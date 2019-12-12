package receive

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"

	"github.com/openshift/telemeter/pkg/authorize"
)

const forwardTimeout = 5 * time.Second
const requestLimit = 15 * 1024 // based on historic Prometheus data with 6KB at most

// ClusterAuthorizer authorizes a cluster by its token and id, returning a subject or error
type ClusterAuthorizer interface {
	AuthorizeCluster(token, cluster string) (subject string, err error)
}

// Handler knows the forwardURL for all requests
type Handler struct {
	ForwardURL   string
	PartitionKey string
	client       *http.Client
	logger       log.Logger

	// Metrics.
	forwardRequestsTotal *prometheus.CounterVec
}

// NewHandler returns a new Handler with a http client
func NewHandler(logger log.Logger, forwardURL string, partitionKey string, reg prometheus.Registerer) *Handler {
	h := &Handler{
		ForwardURL:   forwardURL,
		PartitionKey: partitionKey,
		client: &http.Client{
			Timeout: forwardTimeout,
		},
		logger: log.With(logger, "component", "receive/handler"),
		forwardRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "telemeter_forward_requests_total",
				Help: "The number of forwarded remote-write requests.",
			}, []string{"result"},
		),
	}

	if reg != nil {
		reg.MustRegister(h.forwardRequestsTotal)
	}

	return h
}

// Receive a remote-write request after it has been authenticated and forward it to Thanos
func (h *Handler) Receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Limit the request body size to a sane default
	r.Body = http.MaxBytesReader(w, r.Body, requestLimit)

	err := validateLabels(r, h.PartitionKey)
	if err != nil {
		level.Error(h.logger).Log("msg", "failed to validate labels in request", "err", err)

		if err == ErrRequiredLabelMissing {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
	defer cancel()

	req, err := http.NewRequest(http.MethodPost, h.ForwardURL, r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "failed to create forward request", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req = req.WithContext(ctx)
	req.Header.Add("THANOS-TENANT", r.Context().Value(authorize.TenantKey).(string))

	resp, err := h.client.Do(req)
	if err != nil {
		h.forwardRequestsTotal.WithLabelValues("error").Inc()
		level.Error(h.logger).Log("msg", "failed to forward request", "err", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	if resp.StatusCode/100 != 2 {
		msg := "upstream response status is not 200 OK"
		h.forwardRequestsTotal.WithLabelValues("error").Inc()
		level.Error(h.logger).Log("msg", msg, "statuscode", resp.Status)
		http.Error(w, msg, resp.StatusCode)
		return
	}
	h.forwardRequestsTotal.WithLabelValues("success").Inc()
	w.WriteHeader(resp.StatusCode)
}

var ErrRequiredLabelMissing = fmt.Errorf("a required label is missing from the metric")

// TODO: Make this a middleware eventually
func validateLabels(r *http.Request, partitionKey string) error {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return err
	}

	var wreq prompb.WriteRequest
	if err = proto.Unmarshal(reqBuf, &wreq); err != nil {
		return err
	}

	for _, ts := range wreq.GetTimeseries() {
		for _, l := range ts.Labels {
			if l.Name == partitionKey {
				return nil
			}
		}
	}

	return ErrRequiredLabelMissing
}
