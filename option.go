package otelsql

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// type traceAttributes []attribute.KeyValue

// Config is used to configure the go-restful middleware.
type options struct {
	traceProvider   trace.TracerProvider
	traceAttributes []attribute.KeyValue
}

// Option specifies instrumentation configuration options.
type Option func(*options)

// WithTraceProvider configures the interceptor with the specified trace provider.
func WithTraceProvider(traceProvider trace.TracerProvider) Option {
	return func(o *options) {
		o.traceProvider = traceProvider
	}
}

// WithTraceAttributes configures the interceptor to attach the default KeyValues.
func WithTraceAttributes(traceAttributes []attribute.KeyValue) Option {
	return func(o *options) {
		o.traceAttributes = traceAttributes
	}
}
