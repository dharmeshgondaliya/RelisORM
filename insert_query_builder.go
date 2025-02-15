package RelisORM

import (
	"fmt"
	"strings"
)

type InsertQueryBuilder struct {
	Schema Schema
}

func (i *InsertQueryBuilder) BuildInsertQuery(data Map, returning bool) string {
	var primaryKey *string = i.getPrimaryKeyOfTable()
	var fields []string

	var primaryKeyExist bool = primaryKey != nil && ContainsKey(data, *primaryKey)

	for k := range data {
		if primaryKeyExist && k != *primaryKey {
			fields = append(fields, k)
		}
	}

	var values []string

	for _, field := range fields {
		if data[field] == nil {
			values = append(values, "NULL")
		} else {
			values = append(values, fmt.Sprintf("'%v'", data[field]))
		}
	}

	var query string = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", i.Schema.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
	if returning {
		query += " RETURNING *"
	}
	return query
}

func (i *InsertQueryBuilder) BuildMultiInsertQuery(datas []Map, returning bool) string {
	var primaryKey *string = i.getPrimaryKeyOfTable()

	var fields []string

	for field := range i.Schema.Fields {
		if primaryKey != nil && field != *primaryKey {
			fields = append(fields, field)
		}
	}

	var values []string

	for _, data := range datas {
		var columns []string
		for _, field := range fields {
			if data[field] != nil {
				columns = append(columns, fmt.Sprintf("'%v'", data[field]))
			} else {
				if fieldData, ok := i.Schema.Fields[field].(Map); ok && fieldData["default"] != nil {
					columns = append(columns, fmt.Sprintf("'%v'", fieldData["default"]))
				} else {
					columns = append(columns, "NULL")
				}
			}
		}
		values = append(values, "("+strings.Join(columns, ", ")+")")
	}

	var query string = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", i.Schema.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
	if returning {
		query += " RETURNING *"
	}
	return query
}

func (i *InsertQueryBuilder) getPrimaryKeyOfTable() *string {
	for column, value := range i.Schema.Fields {
		if field, ok := value.(Map); ok {
			if field["primaryKey"] == true && (field["autoIncrement"] == true || field["type"] == "SERIAL" || field["type"] == "BIGSERIAL" || field["type"] == "UUID") {
				return &column
			}
		}
	}
	return nil
}
