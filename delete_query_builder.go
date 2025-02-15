package RelisORM

type deleteQueryBuilder struct {
	Schema Schema
}

func (d *deleteQueryBuilder) buildDeleteQuery(where Where, returning bool) (string, error) {
	var query string = "DELETE FROM " + d.Schema.Table
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
