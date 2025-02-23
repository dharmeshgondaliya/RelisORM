package relisorm

import (
	"fmt"
)

type ORM struct {
	db      DatabaseClient
	schemas map[string]Schema
}

func NewORM(db DatabaseClient, schemas []Schema) (*ORM, error) {
	orm := &ORM{db: db, schemas: make(map[string]Schema)}
	for _, s := range schemas {
		if len(s.Fields) == 0 {
			return nil, fmt.Errorf("at least one field is required for the %s table", s.Table)
		}
		orm.schemas[s.Table] = s
	}
	return orm, nil
}

func (o *ORM) Sync(sync bool) error {

	tables, err := createTableQueries(o.schemas)
	if err != nil {
		return err
	}

	if sync {
		var deleteTables string = deleteAllTablesQuery()
		fmt.Println(deleteTables)
		for _, table := range tables {
			fmt.Println(table)
			if _, err := o.db.Query(table); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *ORM) isValidTableName(table string) error {
	if table == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if _, ok := o.schemas[table]; !ok {
		return fmt.Errorf("%s does not exist", table)
	}
	return nil
}

func (o *ORM) Insert(table string, data Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	if data["data"] == nil {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var insertData Map

	v, ok := data["data"].(Map)

	if !ok || len(v) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	insertData = v

	var returning bool

	if data["returning"] != nil {
		r, ok := data["returning"].(bool)
		if !ok {
			return nil, fmt.Errorf("")
		}
		returning = r
	}

	var queryBuilder insertQueryBuilder = insertQueryBuilder{Schema: o.schemas[table]}
	var query string = queryBuilder.buildInsertQuery(insertData, returning)
	fmt.Println(query)
	return o.db.Query(query)
}

func (o *ORM) MultiInsert(table string, data Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	if data["data"] == nil {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var insertData []Map

	v, ok := data["data"].([]Map)

	if !ok || len(v) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	insertData = v

	var returning bool

	if data["returning"] != nil {
		r, ok := data["returning"].(bool)
		if !ok {
			return nil, fmt.Errorf("")
		}
		returning = r
	}

	var queryBuilder insertQueryBuilder = insertQueryBuilder{Schema: o.schemas[table]}
	var query string = queryBuilder.buildMultiInsertQuery(insertData, returning)
	fmt.Println(query)
	return o.db.Query(query)
}

func (o *ORM) Update(table string, data Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	if data["data"] == nil {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var udpateData Map

	v, ok := data["data"].(Map)

	if !ok || len(v) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	udpateData = v

	var whereData Where

	if data["where"] != nil {
		w, ok := data["where"].(Where)

		if !ok {
			return nil, fmt.Errorf("where type are not match")
		}

		whereData = w
	}

	var returning bool

	if data["returning"] != nil {
		r, ok := data["returning"].(bool)
		if !ok {
			return nil, fmt.Errorf("")
		}
		returning = r
	}

	var queryBuilder updateQueryBuilder = updateQueryBuilder{Schema: o.schemas[table]}
	query, err := queryBuilder.buildUpdateQuery(udpateData, whereData, returning)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return o.db.Query(query)
}

func (o *ORM) Delete(table string, data Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	var whereData Where

	if data["where"] != nil {
		w, ok := data["where"].(Where)

		if !ok {
			return nil, fmt.Errorf("where type are not match")
		}

		whereData = w
	}

	var returning bool

	if data["returning"] != nil {
		r, ok := data["returning"].(bool)
		if !ok {
			return nil, fmt.Errorf("")
		}
		returning = r
	}

	var queryBuilder deleteQueryBuilder = deleteQueryBuilder{Schema: o.schemas[table]}
	query, err := queryBuilder.buildDeleteQuery(whereData, returning)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return o.db.Query(query)
}

func (o *ORM) FindAll(table string, data ...Map) ([]Map, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	var fields []Fields
	var where Where
	var include []Map
	var group []string
	var having Where
	var order Order
	var limit int = -1
	var offset int = -1

	if len(data) != 0 {
		if data[0]["fields"] != nil {
			data, ok := data[0]["fields"].([]Fields)
			if !ok {
				return nil, fmt.Errorf("fields type are not match")
			}
			fields = data
		}
		if data[0]["where"] != nil {
			data, ok := data[0]["where"].(Where)
			if !ok {
				return nil, fmt.Errorf("where type are not match")
			}
			where = data
		}
		if data[0]["limit"] != nil {
			data, ok := data[0]["limit"].(int)
			if !ok {
				return nil, fmt.Errorf("limit must be in integer")
			}
			limit = data
		}
		if data[0]["offset"] != nil {
			data, ok := data[0]["offset"].(int)
			if !ok {
				return nil, fmt.Errorf("offset must be in integer")
			}
			offset = data
		}

		if data[0]["include"] != nil {
			data, ok := data[0]["include"].([]Map)
			if !ok {
				return nil, fmt.Errorf("include type are not match")
			}
			include = data
		}
		if data[0]["group"] != nil {
			data, ok := data[0]["group"].([]string)
			if !ok {
				return nil, fmt.Errorf("group type are not match")
			}
			group = data
		}
		if data[0]["having"] != nil {
			data, ok := data[0]["having"].(Where)
			if !ok {
				return nil, fmt.Errorf("having type are not match")
			}
			having = data
		}
		if data[0]["order"] != nil {
			data, ok := data[0]["order"].(Order)
			if !ok {
				return nil, fmt.Errorf("order type are not match")
			}
			order = data
		}
	}

	var queryBuilder selectQueryBuilder = selectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.buildSelectQuery(table, fields, where, limit, offset, include, group, having, order, false)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	datas, err := o.db.Query(query)
	if err != nil {
		return nil, err
	}
	return queryBuilder.parseData(*datas)
}

func (o *ORM) FindOne(table string, data ...Map) (*Map, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	var fields []Fields
	var where Where
	var include []Map
	var group []string
	var having Where
	var order Order
	var limit int = -1

	if len(data) != 0 {
		if data[0]["fields"] != nil {
			data, ok := data[0]["fields"].([]Fields)
			if !ok {
				return nil, fmt.Errorf("fields type are not match")
			}
			fields = data
		}
		if data[0]["where"] != nil {
			data, ok := data[0]["where"].(Where)
			if !ok {
				return nil, fmt.Errorf("where type are not match")
			}
			where = data
		}
		if data[0]["limit"] != nil {
			data, ok := data[0]["limit"].(int)
			if !ok {
				return nil, fmt.Errorf("limit must be in integer")
			}
			limit = data
		}

		if data[0]["include"] != nil {
			data, ok := data[0]["include"].([]Map)
			if !ok {
				return nil, fmt.Errorf("include type are not match")
			}
			include = data
		}
		if data[0]["group"] != nil {
			data, ok := data[0]["group"].([]string)
			if !ok {
				return nil, fmt.Errorf("group type are not match")
			}
			group = data
		}
		if data[0]["having"] != nil {
			data, ok := data[0]["having"].(Where)
			if !ok {
				return nil, fmt.Errorf("having type are not match")
			}
			having = data
		}
		if data[0]["order"] != nil {
			data, ok := data[0]["order"].(Order)
			if !ok {
				return nil, fmt.Errorf("order type are not match")
			}
			order = data
		}
	}

	var queryBuilder selectQueryBuilder = selectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.buildSelectQuery(table, fields, where, limit, -1, include, group, having, order, false)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	datas, err := o.db.Query(query)
	if err != nil {
		return nil, err
	}
	res, err := queryBuilder.parseData(*datas)

	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return &res[0], nil
}

func (o *ORM) Count(table string, data ...Map) (int64, error) {
	if err := o.isValidTableName(table); err != nil {
		return 0, err
	}

	var fields []Fields
	var where Where
	var include []Map
	var group []string
	var having Where

	if len(data) != 0 {

		if data[0]["fields"] != nil {
			data, ok := data[0]["fields"].([]Fields)
			if !ok {
				return 0, fmt.Errorf("fields type are not match")
			}
			fields = data
		}
		if data[0]["where"] != nil {
			data, ok := data[0]["where"].(Where)
			if !ok {
				return 0, fmt.Errorf("where type are not match")
			}
			where = data
		}
		if data[0]["include"] != nil {
			data, ok := data[0]["include"].([]Map)
			if !ok {
				return 0, fmt.Errorf("include type are not match")
			}
			include = data
		}
		if data[0]["group"] != nil {
			data, ok := data[0]["group"].([]string)
			if !ok {
				return 0, fmt.Errorf("group type are not match")
			}
			group = data
		}
		if data[0]["having"] != nil {
			data, ok := data[0]["having"].(Where)
			if !ok {
				return 0, fmt.Errorf("having type are not match")
			}
			having = data
		}
	}

	var queryBuilder selectQueryBuilder = selectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.buildSelectQuery(table, fields, where, -1, -1, include, group, having, nil, true)
	if err != nil {
		return 0, err
	}
	fmt.Println(query)
	datas, err := o.db.Query(query)
	if err != nil {
		return 0, err
	}
	if len(*datas) > 0 {
		count := (*datas)[0]["count"]
		return count.(int64), nil
	}
	return 0, nil
}

func (o *ORM) FindAndCountAll(table string, data ...Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
	}

	var fields []Fields
	var where Where
	var include []Map
	var group []string
	var having Where
	var order Order
	var limit int = -1
	var offset int = -1

	if len(data) != 0 {

		if data[0]["fields"] != nil {
			data, ok := data[0]["fields"].([]Fields)
			if !ok {
				return nil, fmt.Errorf("fields type are not match")
			}
			fields = data
		}
		if data[0]["where"] != nil {
			data, ok := data[0]["where"].(Where)
			if !ok {
				return nil, fmt.Errorf("where type are not match")
			}
			where = data
		}
		if data[0]["limit"] != nil {
			data, ok := data[0]["limit"].(int)
			if !ok {
				return nil, fmt.Errorf("limit must be in integer")
			}
			limit = data
		}
		if data[0]["offset"] != nil {
			data, ok := data[0]["offset"].(int)
			if !ok {
				return nil, fmt.Errorf("offset must be in integer")
			}
			offset = data
		}

		if data[0]["include"] != nil {
			data, ok := data[0]["include"].([]Map)
			if !ok {
				return nil, fmt.Errorf("include type are not match")
			}
			include = data
		}
		if data[0]["group"] != nil {
			data, ok := data[0]["group"].([]string)
			if !ok {
				return nil, fmt.Errorf("group type are not match")
			}
			group = data
		}
		if data[0]["having"] != nil {
			data, ok := data[0]["having"].(Where)
			if !ok {
				return nil, fmt.Errorf("having type are not match")
			}
			having = data
		}
		if data[0]["order"] != nil {
			data, ok := data[0]["order"].(Order)
			if !ok {
				return nil, fmt.Errorf("order type are not match")
			}
			order = data
		}
	}

	var findQueryBuilder selectQueryBuilder = selectQueryBuilder{Schemas: o.schemas}
	findQuery, findQueryError := findQueryBuilder.buildSelectQuery(table, fields, where, limit, offset, include, group, having, order, false)
	var countQueryBuilder selectQueryBuilder = selectQueryBuilder{Schemas: o.schemas}
	countQuery, countQueryError := countQueryBuilder.buildSelectQuery(table, fields, where, -1, -1, include, group, having, order, true)
	if findQueryError != nil {
		return nil, findQueryError
	}
	if countQueryError != nil {
		return nil, countQueryError
	}
	fmt.Println(findQuery)
	fmt.Println(countQuery)

	findData, findErr := o.db.Query(findQuery)
	countData, countErr := o.db.Query(countQuery)

	if findErr != nil {
		return nil, findErr
	}
	if countErr != nil {
		return nil, countErr
	}
	res, findParseErr := findQueryBuilder.parseData(*findData)
	if findParseErr != nil {
		return nil, findParseErr
	}
	var count int64
	if len(*countData) > 0 {
		count = (*countData)[0]["count"].(int64)
	}
	return Map{"count": count, "rows": res}, nil
}
