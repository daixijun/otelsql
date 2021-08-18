package otelsql

import (
	"database/sql/driver"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
)

func paramsAttr(args []driver.Value) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(args))
	for i, arg := range args {
		key := "db.arg." + strconv.Itoa(i)
		attrs = append(attrs, attribute.Any(key, arg))
	}
	return attrs
}

func namedParamsAttr(args []driver.NamedValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(args))
	for _, arg := range args {
		var key string
		if arg.Name != "" {
			key = arg.Name
		} else {
			key = "db.arg." + strconv.Itoa(arg.Ordinal)
		}
		attrs = append(attrs, attribute.Any(key, arg.Value))
	}
	return attrs
}
