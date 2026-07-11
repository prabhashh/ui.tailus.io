// Package tracing wires up OpenTelemetry so a single check's journey
// (scheduler dispatch -> JetStream -> prober -> JetStream -> result
// processor -> Postgres) can be followed as one distributed trace. Exporting
// is OTLP/HTTP so it works against any modern collector (Grafana Tempo,
// Jaeger, Honeycomb, an OTel Collector fronting all three).
package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup configures the global tracer provider. If endpoint is empty, tracing
// is a no-op (spans are created but never exported) — useful for local dev
// without standing up a collector. Returns a shutdown func to flush/close on
// process exit.
func Setup(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))

	if endpoint != "" {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
		if err != nil {
			return nil, err
		}
		// BatchSpanProcessor keeps export overhead off the hot path: spans
		// are queued and flushed on a timer/batch-size trigger, not
		// synchronously per check.
		opts = append(opts, sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
