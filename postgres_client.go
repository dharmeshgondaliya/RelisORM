package RelisORM

import (
	"database/sql"
)

type PostgresClient struct {
	DatabaseClient
	DB *sql.DB
}

func (p *PostgresClient) Close() error {
	return p.DB.Close()
}

func (p *PostgresClient) Ping() error {
	return p.DB.Ping()
}

func (p *PostgresClient) Query(query string) (*[]Map, error) {

	rows, err := p.DB.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []Map

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{}, 0)
		for i, col := range cols {
			var value interface{}
			val := values[i]
			if b, ok := val.(byte); ok {
				value = string(b)
			} else {
				value = val
			}
			rowMap[col] = value
		}

		result = append(result, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &result, nil
}
