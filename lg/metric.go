package lg

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
)

var meterKey = contextKey{"meter"}
var promHTTPKey = contextKey{"promHTTP"}

func Meter(ctx context.Context) api.Meter {
	if t := fromContext[contextKey, api.Meter](ctx, tracerKey); t != nil {
		return t
	}

	return otel.Meter("")
}
func NewHTTP(ctx context.Context) *httpHandle {
	t := fromContext[contextKey, *prometheus.Exporter](ctx, promHTTPKey)
	return &httpHandle{t}
}

func initMetrics(ctx context.Context, name string) (context.Context, func() error) {
	// goversion := ""
	// pkg := ""
	// host := ""
	// if info, ok := debug.ReadBuildInfo(); ok {
	// 	goversion = info.GoVersion
	// 	pkg = info.Path
	// }
	// if h, err := os.Hostname(); err == nil {
	// 	host = h
	// }

	// config := prometheus.Config{
	// 	DefaultHistogramBoundaries: []float64{
	// 		2 << 6, 2 << 8, 2 << 10, 2 << 12, 2 << 14, 2 << 16, 2 << 18, 2 << 20, 2 << 22, 2 << 24, 2 << 26, 2 << 28,
	// 	},
	// }
	// cont := controller.New(
	// 	processor.NewFactory(
	// 		selector.NewWithHistogramDistribution(
	// 			histogram.WithExplicitBoundaries(config.DefaultHistogramBoundaries),
	// 		),
	// 		aggregation.CumulativeTemporalitySelector(),
	// 		processor.WithMemory(true),
	// 	),
	// 	controller.WithResource(
	// 		resource.NewWithAttributes(
	// 			semconv.SchemaURL,
	// 			attribute.String("app", name),
	// 			attribute.String("host", host),
	// 			attribute.String("go_version", goversion),
	// 			attribute.String("pkg", pkg),
	// 		),
	// 	),
	// )
	ex, err := prometheus.New()
	if err != nil {
		return ctx, nil
	}
	provider := sdk.NewMeterProvider(sdk.WithReader(ex))
	meter := provider.Meter(name)


	ctx = toContext(ctx, promHTTPKey, ex)
	ctx = toContext(ctx, meterKey, meter)
	runtime.Start()

	return ctx, func() error {
		_, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		defer log.Println("metrics stopped")
		return nil
	}
}

type httpHandle struct {
	exp *prometheus.Exporter
}

func (h *httpHandle) RegisterHTTP(mux *http.ServeMux) {
	if h.exp == nil {
		return
	}
	mux.Handle("/metrics", promhttp.Handler())
}
