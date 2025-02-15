package RelisORM

import (
	"database/sql"
)

func OpenPostgresClient(connString string) (DatabaseClient, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}
	return &PostgresClient{DB: db}, nil
}

type DatabaseClient interface {
	Close() error
	Ping() error
	Query(query string) (*[]Map, error)
}
