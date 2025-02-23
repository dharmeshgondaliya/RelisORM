package relisorm

import (
	"fmt"
	"strings"
)

type updateQueryBuilder struct {
	Schema Schema
}

func (u *updateQueryBuilder) buildUpdateQuery(data Map, where Where, returning bool) (string, error) {
	var fields []string
	for k, v := range data {
		fields = append(fields, fmt.Sprintf("%s='%v'", k, v))
	}
	var query string = fmt.Sprintf("UPDATE %s SET %s", u.Schema.Table, strings.Join(fields, ", "))
	if len(where) != 0 {
		_condition, err := getWhereCondition("", where)
		if err != nil {
			return "", err
		}
		query += " WHERE " + _condition
	}
	if returning {
		query += " RETURNING *"
	}
	return query, nil
}
