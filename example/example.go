package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/daixijun/otelsql"
	"github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func main() {
	exp, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		log.Fatalf("failed to initialize stdout export: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(semconv.ServiceNameKey.String("example")),
	)

	if err != nil {
		log.Fatalf("failed to merge resource: %s", err.Error())
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	ctx, span := tp.Tracer("exmaple").Start(ctx, "otelexample")
	defer span.End()

	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		// ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		// defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			panic(err)
		}
	}(ctx)

	driverName := otelsql.Register(
		"sqlite3",
		&sqlite3.SQLiteDriver{},
		// otelsql.WithTraceProvider(tp),
	)

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	sqlStmt := `
	create table bar (id integer not null primary key, name text);
	delete from bar;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		panic(err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		panic(err)
	}
	stmt, err := tx.PrepareContext(ctx, "insert into bar(id, name) values(?, ?)")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for i := 0; i < 3; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("otelsql-%v", i))
		if err != nil {
			panic(err)
		}
	}
	_ = tx.Commit()

	rows, err := db.QueryContext(ctx, "select id, name from bar")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			panic(err)
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}

	stmt, err = db.PrepareContext(ctx, "select name from bar where id = ?")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("2").Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)

	_, err = db.ExecContext(ctx, "delete from bar")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "insert into bar(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		log.Fatal(err)
	}

	rows, err = db.Query("select id, name from bar")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
