package main

import (
	"context"
	"fmt"
	stdlog "log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/oklog/run"
	"github.com/prometheus/common/expfmt"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/openshift/telemeter/pkg/forwarder"
	telemeterhttp "github.com/openshift/telemeter/pkg/http"
	"github.com/openshift/telemeter/pkg/logger"
	"github.com/openshift/telemeter/pkg/metricfamily"
	"github.com/openshift/telemeter/pkg/tracing"
)

func main() {
	opt := &Options{
		Listen:     "localhost:9002",
		LimitBytes: 200 * 1024,
		Rules:      []string{`{__name__="up"}`},
		Interval:   4*time.Minute + 30*time.Second,
	}
	cmd := &cobra.Command{
		Short:         "Federate Prometheus via push",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return opt.Run()
		},
	}

	cmd.Flags().StringVar(&opt.Listen, "listen", opt.Listen, "A host:port to listen on for health and metrics.")
	cmd.Flags().StringVar(&opt.From, "from", opt.From, "The Prometheus server to federate from.")

	// TLS flags
	cmd.Flags().StringVar(&opt.TLSCertFile, "tls-cert-file", "", "File containing the x509 public certificate to use when connecting to the Prometheus server.")
	cmd.Flags().StringVar(&opt.TLSKey, "tls-private-key-file", "", "File containing the x509 private key to use when connecting to the Prometheus server. It should match with --tls-cert-file.")

	cmd.Flags().StringVar(&opt.FromToken, "from-token", opt.FromToken, "A bearer token to use when authenticating to the source Prometheus server.")
	cmd.Flags().StringVar(&opt.FromCAFile, "from-ca-file", opt.FromCAFile, "A file containing the CA certificate to use to verify the --from URL in addition to the system roots certificates.")
	cmd.Flags().StringVar(&opt.FromTokenFile, "from-token-file", opt.FromTokenFile, "A file containing a bearer token to use when authenticating to the source Prometheus server.")
	cmd.Flags().StringVar(&opt.Identifier, "id", opt.Identifier, "The unique identifier for metrics sent with this client.")
	cmd.Flags().StringVar(&opt.To, "to", opt.To, "A telemeter server to send metrics to.")
	cmd.Flags().StringVar(&opt.ToUpload, "to-upload", opt.ToUpload, "A telemeter server endpoint to push metrics to. Will be defaulted for standard servers.")
	cmd.Flags().StringVar(&opt.ToAuthorize, "to-auth", opt.ToAuthorize, "A telemeter server endpoint to exchange the bearer token for an access token. Will be defaulted for standard servers.")
	cmd.Flags().StringVar(&opt.ToToken, "to-token", opt.ToToken, "A bearer token to use when authenticating to the destination telemeter server.")
	cmd.Flags().StringVar(&opt.ToTokenFile, "to-token-file", opt.ToTokenFile, "A file containing a bearer token to use when authenticating to the destination telemeter server.")
	cmd.Flags().DurationVar(&opt.Interval, "interval", opt.Interval, "The interval between scrapes. Prometheus returns the last 5 minutes of metrics when invoking the federation endpoint.")
	cmd.Flags().Int64Var(&opt.LimitBytes, "limit-bytes", opt.LimitBytes, "The maxiumum acceptable size of a response returned when scraping Prometheus.")

	// TODO: more complex input definition, such as a JSON struct
	cmd.Flags().StringArrayVar(&opt.Rules, "match", opt.Rules, "Match rules to federate.")
	cmd.Flags().StringVar(&opt.RulesFile, "match-file", opt.RulesFile, "A file containing match rules to federate, one rule per line.")

	cmd.Flags().StringSliceVar(&opt.LabelFlag, "label", opt.LabelFlag, "Labels to add to each outgoing metric, in key=value form.")
	cmd.Flags().StringSliceVar(&opt.RenameFlag, "rename", opt.RenameFlag, "Rename metrics before sending by specifying OLD=NEW name pairs. Defaults to renaming ALERTS to alerts. Defaults to ALERTS=alerts.")

	cmd.Flags().StringSliceVar(&opt.AnonymizeLabels, "anonymize-labels", opt.AnonymizeLabels, "Anonymize the values of the provided values before sending them on.")
	cmd.Flags().StringVar(&opt.AnonymizeSalt, "anonymize-salt", opt.AnonymizeSalt, "A secret and unguessable value used to anonymize the input data.")
	cmd.Flags().StringVar(&opt.AnonymizeSaltFile, "anonymize-salt-file", opt.AnonymizeSaltFile, "A file containing a secret and unguessable value used to anonymize the input data.")

	cmd.Flags().BoolVarP(&opt.Verbose, "verbose", "v", opt.Verbose, "Show verbose output.")

	cmd.Flags().StringVar(&opt.LogLevel, "log-level", opt.LogLevel, "Log filtering level. e.g info, debug, warn, error")

	cmd.Flags().StringVar(&opt.TracingServiceName, "internal.tracing.service-name", "telemeter-client",
		"The service name to report to the tracing backend.")
	cmd.Flags().StringVar(&opt.TracingEndpoint, "internal.tracing.endpoint", "",
		"The full URL of the trace collector. If it's not set, tracing will be disabled.")
	cmd.Flags().Float64Var(&opt.TracingSamplingFraction, "internal.tracing.sampling-fraction", 0.1,
		"The fraction of traces to sample. Thus, if you set this to .5, half of traces will be sampled.")
	cmd.Flags().StringVar(&opt.TracingEndpointType, "internal.tracing.endpoint-type", string(tracing.EndpointTypeAgent),
		fmt.Sprintf("The tracing endpoint type. Options: '%s', '%s'.", tracing.EndpointTypeAgent, tracing.EndpointTypeCollector))

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	lvl, err := cmd.Flags().GetString("log-level")
	if err != nil {
		level.Error(l).Log("msg", "could not parse log-level.")
	}
	l = level.NewFilter(l, logger.LogLevelFromString(lvl))
	l = log.WithPrefix(l, "ts", log.DefaultTimestampUTC)
	l = log.WithPrefix(l, "caller", log.DefaultCaller)
	stdlog.SetOutput(log.NewStdlibAdapter(l))
	opt.Logger = l
	level.Info(l).Log("msg", "telemeter client initialized")

	if err := cmd.Execute(); err != nil {
		level.Error(l).Log("err", err)
		os.Exit(1)
	}
}

type Options struct {
	Listen     string
	LimitBytes int64
	Verbose    bool

	From          string
	To            string
	TLSCertFile   string
	TLSKey        string
	ToUpload      string
	ToAuthorize   string
	FromCAFile    string
	FromToken     string
	FromTokenFile string
	ToToken       string
	ToTokenFile   string
	Identifier    string

	RenameFlag []string
	Renames    map[string]string

	AnonymizeLabels   []string
	AnonymizeSalt     string
	AnonymizeSaltFile string

	Rules     []string
	RulesFile string

	LabelFlag []string
	Labels    map[string]string

	Interval time.Duration

	LogLevel string
	Logger   log.Logger

	TracingServiceName      string
	TracingEndpoint         string
	TracingEndpointType     string
	TracingSamplingFraction float64
}

func (o *Options) Run() error {
	if len(o.From) == 0 {
		return fmt.Errorf("you must specify a Prometheus server to federate from (e.g. http://localhost:9090)")
	}

	for _, flag := range o.LabelFlag {
		values := strings.SplitN(flag, "=", 2)
		if len(values) != 2 {
			return fmt.Errorf("--label must be of the form key=value: %s", flag)
		}
		if o.Labels == nil {
			o.Labels = make(map[string]string)
		}
		o.Labels[values[0]] = values[1]
	}

	if len(o.RenameFlag) == 0 {
		o.RenameFlag = []string{"ALERTS=alerts"}
	}
	for _, flag := range o.RenameFlag {
		if len(flag) == 0 {
			continue
		}
		values := strings.SplitN(flag, "=", 2)
		if len(values) != 2 {
			return fmt.Errorf("--rename must be of the form OLD_NAME=NEW_NAME: %s", flag)
		}
		if o.Renames == nil {
			o.Renames = make(map[string]string)
		}
		o.Renames[values[0]] = values[1]
	}

	from, err := url.Parse(o.From)
	if err != nil {
		return fmt.Errorf("--from is not a valid URL: %v", err)
	}
	from.Path = strings.TrimRight(from.Path, "/")
	if len(from.Path) == 0 {
		from.Path = "/federate"
	}

	var to, toUpload, toAuthorize *url.URL
	if len(o.ToUpload) > 0 {
		to, err = url.Parse(o.ToUpload)
		if err != nil {
			return fmt.Errorf("--to-upload is not a valid URL: %v", err)
		}
	}
	if len(o.ToAuthorize) > 0 {
		toAuthorize, err = url.Parse(o.ToAuthorize)
		if err != nil {
			return fmt.Errorf("--to-auth is not a valid URL: %v", err)
		}
	}
	if len(o.To) > 0 {
		to, err = url.Parse(o.To)
		if err != nil {
			return fmt.Errorf("--to is not a valid URL: %v", err)
		}
		if len(to.Path) == 0 {
			to.Path = "/"
		}
		if toAuthorize == nil {
			u := *to
			u.Path = path.Join(to.Path, "authorize")
			if len(o.Identifier) > 0 {
				q := to.Query()
				q.Add("id", o.Identifier)
				u.RawQuery = q.Encode()
			}
			toAuthorize = &u
		}
		u := *to
		u.Path = path.Join(to.Path, "upload")
		toUpload = &u
	}

	if toUpload == nil || toAuthorize == nil {
		return fmt.Errorf("either --to or --to-auth and --to-upload must be specified")
	}

	var transformer metricfamily.MultiTransformer

	if len(o.Labels) > 0 {
		transformer.WithFunc(func() metricfamily.Transformer {
			return metricfamily.NewLabel(o.Labels, nil)
		})
	}

	if len(o.Renames) > 0 {
		transformer.WithFunc(func() metricfamily.Transformer {
			return metricfamily.RenameMetrics{Names: o.Renames}
		})
	}

	transformer.WithFunc(func() metricfamily.Transformer {
		return metricfamily.NewDropInvalidFederateSamples(time.Now().Add(-24 * time.Hour))
	})

	transformer.With(metricfamily.TransformerFunc(metricfamily.PackMetrics))
	transformer.With(metricfamily.TransformerFunc(metricfamily.SortMetrics))

	ctx := context.Background()

	tp, err := tracing.InitTracer(
		ctx,
		o.TracingServiceName,
		o.TracingEndpoint,
		o.TracingEndpointType,
		o.TracingSamplingFraction,
	)
	if err != nil {
		return fmt.Errorf("cannot initialize tracer: %v", err)
	}

	otel.SetErrorHandler(tracing.OtelErrorHandler{Logger: o.Logger})

	cfg := forwarder.Config{
		From:          from,
		ToAuthorize:   toAuthorize,
		ToUpload:      toUpload,
		FromToken:     o.FromToken,
		ToToken:       o.ToToken,
		FromTokenFile: o.FromTokenFile,
		ToTokenFile:   o.ToTokenFile,
		FromCAFile:    o.FromCAFile,
		TLSCertFile:   o.TLSCertFile,
		TLSKey:        o.TLSKey,

		AnonymizeLabels:   o.AnonymizeLabels,
		AnonymizeSalt:     o.AnonymizeSalt,
		AnonymizeSaltFile: o.AnonymizeSaltFile,
		Debug:             o.Verbose,
		Interval:          o.Interval,
		LimitBytes:        o.LimitBytes,
		Rules:             o.Rules,
		RulesFile:         o.RulesFile,
		Transformer:       transformer,

		Logger: o.Logger,
		Tracer: tp,
	}

	worker, err := forwarder.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to configure Telemeter client: %v", err)
	}

	level.Info(o.Logger).Log("msg", "starting telemeter-client", "from", o.From, "to", o.To, "listen", o.Listen)

	var g run.Group
	{
		// Execute the worker's `Run` func.
		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			worker.Run(ctx)
			return nil
		}, func(error) {
			cancel()
		})
	}

	{
		// Notify and reload on SIGHUP.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		cancel := make(chan struct{})
		g.Add(func() error {
			for {
				select {
				case <-hup:
					if err := worker.Reconfigure(cfg); err != nil {
						level.Error(o.Logger).Log("msg", "failed to reload config", "err", err)
						return err
					}
				case <-cancel:
					return nil
				}
			}
		}, func(error) {
			close(cancel)
		})
	}

	if len(o.Listen) > 0 {
		handlers := http.NewServeMux()
		telemeterhttp.DebugRoutes(handlers)
		telemeterhttp.HealthRoutes(handlers)
		telemeterhttp.MetricRoutes(handlers)
		telemeterhttp.ReloadRoutes(handlers, func() error {
			return worker.Reconfigure(cfg)
		})
		handlers.Handle("/federate",
			otelhttp.NewHandler(serveLastMetrics(o.Logger, worker), "federate", otelhttp.WithTracerProvider(tp)),
		)
		l, err := net.Listen("tcp", o.Listen)
		if err != nil {
			return fmt.Errorf("failed to listen: %v", err)
		}

		{
			// Run the HTTP server.
			g.Add(func() error {
				if err := http.Serve(l, handlers); err != nil && err != http.ErrServerClosed {
					level.Error(o.Logger).Log("msg", "server exited unexpectedly", "err", err)
					return err
				}
				return nil
			}, func(error) {
				l.Close()
			})
		}
	}

	return g.Run()
}

// serveLastMetrics retrieves the last set of metrics served
func serveLastMetrics(l log.Logger, worker *forwarder.Worker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		families := worker.LastMetrics()
		w.Header().Set("Content-Type", string(expfmt.FmtText))
		encoder := expfmt.NewEncoder(w, expfmt.FmtText)
		for _, family := range families {
			if family == nil {
				continue
			}
			if err := encoder.Encode(family); err != nil {
				level.Error(l).Log("msg", "unable to write metrics for family", "err", err)
				break
			}
		}
	})
}
