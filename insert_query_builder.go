package relisorm

import (
	"fmt"
	"strings"
)

type insertQueryBuilder struct {
	Schema Schema
}

func (i *insertQueryBuilder) buildInsertQuery(data Map, returning bool) string {
	var primaryKey *string = i.getPrimaryKeyOfTable()
	var fields []string
	var values []string

	for k := range data {
		if k != *primaryKey {
			fields = append(fields, k)
			if data[k] == nil {
				values = append(values, "NULL")
			} else {
				values = append(values, fmt.Sprintf("'%v'", data[k]))
			}
		}
	}

	var query string = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Schema.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
	if returning {
		query += " RETURNING *"
	}
	return query
}

func (i *insertQueryBuilder) buildMultiInsertQuery(datas []Map, returning bool) string {
	var primaryKey *string = i.getPrimaryKeyOfTable()

	var fields []string

	for field := range i.Schema.Fields {
		if field != *primaryKey {
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

	var query string = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Schema.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
	if returning {
		query += " RETURNING *"
	}
	return query
}

func (i *insertQueryBuilder) getPrimaryKeyOfTable() *string {
	for column, value := range i.Schema.Fields {
		if field, ok := value.(Map); ok {
			if field["primaryKey"] == true && (field["autoIncrement"] == true || field["auto_increment"] == true || field["type"].(DataType).Value == "SERIAL" || field["type"].(DataType).Value == "BIGSERIAL" || field["type"].(DataType).Value == "UUID") {
				return &column
			}
		}
	}
	return nil
}
