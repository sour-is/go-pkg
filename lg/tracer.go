// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package lg

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.sour.is/pkg/env"
)

type contextKey struct {
	name string
}

var tracerKey = contextKey{"tracer"}

func Tracer(ctx context.Context) trace.Tracer {
	if t := fromContext[contextKey, trace.Tracer](ctx, tracerKey); t != nil {
		return t
	}
	return otel.Tracer("")
}

func attrs(ctx context.Context) (string, []attribute.KeyValue) {
	var attrs []attribute.KeyValue
	var name string
	if pc, file, line, ok := runtime.Caller(2); ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			name = fn.Name()
		}
		attrs = append(attrs,
			attribute.String("pc", fmt.Sprintf("%v", pc)),
			attribute.String("file", file),
			attribute.Int("line", line),
		)
	}
	return name, attrs
}

type wrapSpan struct {
	trace.Span
}

func LogQuery(q string, args []any, err error) (string, trace.EventOption) {
	var attrs []attribute.KeyValue
	for k, v := range args {
		var attr attribute.KeyValue
		switch v:=v.(type) {
		case int64:
			attr = attribute.Int64(
				fmt.Sprintf("$%d", k),
				v,
			)
		case string:
			attr = attribute.String(
				fmt.Sprintf("$%d", k),
				v,
			)
		default:
			attr = attribute.String(
				fmt.Sprintf("$%d", k),
				fmt.Sprint(v),
			)
		}

		attrs = append(attrs, attr)
	}
	
	return q, trace.WithAttributes(attrs...)
}

func (w wrapSpan) AddEvent(name string, options ...trace.EventOption) {
	w.Span.AddEvent(name, options...)

	cfg := trace.NewEventConfig(options...)

	attrs := cfg.Attributes()
	args := make([]any, len(attrs))

	for i, a := range attrs {
		switch a.Value.Type() {
		case attribute.BOOL:
			args[i] = slog.Bool(string(a.Key), a.Value.AsBool())
		case attribute.INT64:
			args[i] = slog.Int64(string(a.Key), a.Value.AsInt64())
		case attribute.FLOAT64:
			args[i] = slog.Float64(string(a.Key), a.Value.AsFloat64())
		case attribute.STRING:
			args[i] = slog.String(string(a.Key), a.Value.AsString())
		default:
			args[i] = slog.Any(string(a.Key), a.Value.AsInterface())
		}
		
	}

	slog.Debug(name, args...)
}

func (w wrapSpan) RecordError(err error, options ...trace.EventOption) {
	w.Span.RecordError(err, options...)

	if err == nil {
		return
	}
	cfg := trace.NewEventConfig(options...)

	attrs := cfg.Attributes()
	args := make([]any, len(attrs)*2)

	for i, a := range attrs {
		args[2*i] = a.Key
		args[2*i+1] = a.Value
	}

	slog.Error(err.Error(), args...)
}

func Span(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	name, attrs := attrs(ctx)
	attrs = append(attrs, attribute.String("name", name))
	ctx, span := Tracer(ctx).Start(ctx, name, opts...)
	span = &wrapSpan{span}

	span.SetAttributes(attrs...)

	return ctx, span
}
func NamedSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	_, attrs := attrs(ctx)
	attrs = append(attrs, attribute.String("name", name))
	ctx, span := Tracer(ctx).Start(ctx, name, opts...)
	span.SetAttributes(attrs...)

	return ctx, span
}

func Fork(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	name, attrs := attrs(ctx)
	childCTX, childSpan := Tracer(ctx).Start(context.Background(), name, append(opts, trace.WithLinks(trace.LinkFromContext(ctx)))...)
	childSpan.SetAttributes(attrs...)

	_, span := Tracer(ctx).Start(ctx, name, append(opts, trace.WithLinks(trace.LinkFromContext(childCTX)))...)
	span.SetAttributes(attrs...)
	defer span.End()

	return childCTX, childSpan
}

type SampleRate string

const (
	SampleAlways SampleRate = "always"
	SampleNever  SampleRate = "never"
)

func initTracing(ctx context.Context, name string) (context.Context, func() error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(name),
		),
	)
	if err != nil {
		log.Println(wrap(err, "failed to create trace resource"))
		return ctx, nil
	}

	exporterAddr := env.Default("TRACE_ENDPOINT", "")
	if exporterAddr == "" {
		return ctx, nil
	}
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(exporterAddr),
	)
	if err != nil {
		log.Println(wrap(err, "failed to create trace exporter"))
		return ctx, nil
	}
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)

	var sample sdktrace.TracerProviderOption
	sampleRate := SampleRate(env.Default("EV_TRACE_SAMPLE", string(SampleNever)))
	switch sampleRate {
	case "always":
		sample = sdktrace.WithSampler(sdktrace.AlwaysSample())
	case "never":
		sample = sdktrace.WithSampler(sdktrace.NeverSample())
	default:
		if v, err := strconv.Atoi(string(sampleRate)); err != nil {
			sample = sdktrace.WithSampler(sdktrace.NeverSample())
		} else {
			sample = sdktrace.WithSampler(sdktrace.TraceIDRatioBased(float64(v) * 0.01))
		}
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sample,
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx = toContext(ctx, tracerKey, otel.Tracer(name))

	return ctx, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		defer log.Println("tracer stopped")
		return wrap(tracerProvider.Shutdown(ctx), "failed to shutdown TracerProvider")
	}
}

func wrap(err error, s string) error {
	if err != nil {
		return fmt.Errorf(s, err)
	}
	return nil
}
func reverse[T any](s []T) {
	first, last := 0, len(s)-1
	for first < last {
		s[first], s[last] = s[last], s[first]
		first++
		last--
	}
}

func Htrace(h http.Handler, name string) http.Handler {
	return otelhttp.NewHandler(h, name, otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
		return fmt.Sprintf("%s: %s", operation, r.RequestURI)
	}))
}

func toContext[K comparable, V any](ctx context.Context, key K, value V) context.Context {
	return context.WithValue(ctx, key, value)
}
func fromContext[K comparable, V any](ctx context.Context, key K) V {
	var empty V
	if v, ok := ctx.Value(key).(V); ok {
		return v
	}
	return empty
}
