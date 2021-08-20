package otelsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ngrok/sqlmw"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTracerName = "github.com/daixijun/otelsql"
)

var _ sqlmw.Interceptor = (*sqlInterceptor)(nil)

func Register(drivername string, dri driver.Driver, opts ...Option) string {
	newname := fmt.Sprintf("otel-%s", drivername)

	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.traceProvider == nil {
		o.traceProvider = otel.GetTracerProvider()
	}
	o.traceAttributes = append(o.traceAttributes, attribute.String("db.system", drivername))
	sqlInt := &sqlInterceptor{
		tracer:          o.traceProvider.Tracer(defaultTracerName),
		traceAttributes: o.traceAttributes,
	}
	sql.Register(newname, sqlmw.Driver(dri, sqlInt))
	return newname
}

type sqlInterceptor struct {
	tracer          trace.Tracer
	traceAttributes []attribute.KeyValue
}

func (in *sqlInterceptor) ConnBeginTx(ctx context.Context, conn driver.ConnBeginTx, txOpts driver.TxOptions) (driver.Tx, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = in.tracer.Start(ctx, "ConnBeginTx", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return conn.BeginTx(ctx, txOpts)
}

func (in *sqlInterceptor) ConnPrepareContext(ctx context.Context, conn driver.ConnPrepareContext, query string) (driver.Stmt, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		traceAttributes := append(in.traceAttributes, attribute.String("db.statement", query))
		ctx, span = in.tracer.Start(ctx, "ConnPrepareContext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(traceAttributes...))
	}
	defer span.End()
	return conn.PrepareContext(ctx, query)
}

func (in *sqlInterceptor) ConnPing(ctx context.Context, conn driver.Pinger) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = in.tracer.Start(ctx, "ConnPing", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return conn.Ping(ctx)
}

func (in *sqlInterceptor) ConnExecContext(ctx context.Context, conn driver.ExecerContext, query string, args []driver.NamedValue) (driver.Result, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		traceAttributes := append(in.traceAttributes, attribute.String("db.statement", query))
		traceAttributes = append(traceAttributes, namedParamsAttr(args)...)
		ctx, span = in.tracer.Start(ctx, "ConnExecContext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(traceAttributes...))
	}
	defer span.End()
	return conn.ExecContext(ctx, query, args)
}

func (in *sqlInterceptor) ConnQueryContext(ctx context.Context, conn driver.QueryerContext, query string, args []driver.NamedValue) (driver.Rows, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		traceAttributes := append(in.traceAttributes, attribute.String("db.statement", query))
		traceAttributes = append(traceAttributes, namedParamsAttr(args)...)
		ctx, span = in.tracer.Start(ctx, "ConnQueryContext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(traceAttributes...))
	}
	defer span.End()
	return conn.QueryContext(ctx, query, args)
}

func (in *sqlInterceptor) ConnectorConnect(ctx context.Context, connect driver.Connector) (driver.Conn, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = in.tracer.Start(ctx, "ConnectorConnect", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return connect.Connect(ctx)
}

func (in *sqlInterceptor) ResultLastInsertId(res driver.Result) (int64, error) {
	// ctx := context.Background()
	// _, span := in.tracer.Start(ctx, "ResultLastInsertId", trace.WithSpanKind(trace.SpanKindClient),
	// 	trace.WithAttributes(in.traceAttributes...))
	// defer span.End()
	return res.LastInsertId()
}

func (in *sqlInterceptor) ResultRowsAffected(res driver.Result) (int64, error) {
	// ctx := context.Background()
	// _, span := in.tracer.Start(ctx, "ResultRowsAffected", trace.WithSpanKind(trace.SpanKindClient),
	// 	trace.WithAttributes(in.traceAttributes...))
	// defer span.End()
	return res.RowsAffected()
}

func (in *sqlInterceptor) RowsNext(ctx context.Context, rows driver.Rows, dest []driver.Value) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		_, span = in.tracer.Start(ctx, "RowsNext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return rows.Next(dest)
}

func (in *sqlInterceptor) StmtExecContext(ctx context.Context, stmt driver.StmtExecContext, _ string, args []driver.NamedValue) (driver.Result, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		traceAttributes := append(in.traceAttributes, namedParamsAttr(args)...)
		ctx, span = in.tracer.Start(ctx, "StmtExecContext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(traceAttributes...))
	}
	defer span.End()
	return stmt.ExecContext(ctx, args)
}

func (in *sqlInterceptor) StmtQueryContext(ctx context.Context, stmt driver.StmtQueryContext, _ string, args []driver.NamedValue) (driver.Rows, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {

		traceAttributes := append(in.traceAttributes, namedParamsAttr(args)...)
		ctx, span = in.tracer.Start(ctx, "StmtQueryContext", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(traceAttributes...))
	}

	defer span.End()
	return stmt.QueryContext(ctx, args)
}

func (in *sqlInterceptor) StmtClose(ctx context.Context, stmt driver.Stmt) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		_, span = in.tracer.Start(ctx, "StmtClose", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return stmt.Close()
}

func (in *sqlInterceptor) TxCommit(ctx context.Context, tx driver.Tx) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		_, span = in.tracer.Start(ctx, "TxCommit", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return tx.Commit()
}

func (in *sqlInterceptor) TxRollback(ctx context.Context, tx driver.Tx) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		_, span = in.tracer.Start(ctx, "TxRollback", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(in.traceAttributes...))
	}
	defer span.End()
	return tx.Rollback()
}
