package RelisORM

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

	tables, err := CreateTableQueries(o.schemas)
	if err != nil {
		return err
	}

	if sync {
		var deleteTables string = DeleteAllTablesQuery()
		fmt.Println(deleteTables)
		for _, table := range tables {
			fmt.Print(table)
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

	var queryBuilder InsertQueryBuilder = InsertQueryBuilder{Schema: o.schemas[table]}
	var query string = queryBuilder.BuildInsertQuery(insertData, returning)
	fmt.Println(query)
	return nil, nil
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

	var queryBuilder InsertQueryBuilder = InsertQueryBuilder{Schema: o.schemas[table]}
	var query string = queryBuilder.BuildMultiInsertQuery(insertData, returning)
	fmt.Println(query)
	return nil, nil
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

	var queryBuilder UpdateQueryBuilder = UpdateQueryBuilder{Schema: o.schemas[table]}
	query, err := queryBuilder.BuildUpdateQuery(udpateData, whereData, returning)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return nil, nil
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

	var queryBuilder DeleteQueryBuilder = DeleteQueryBuilder{Schema: o.schemas[table]}
	query, err := queryBuilder.BuildDeleteQuery(whereData, returning)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return nil, nil
}

func (o *ORM) FindAll(table string, data ...Map) (any, error) {
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

	var queryBuilder SelectQueryBuilder = SelectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.BuildSelectQuery(table, fields, where, limit, offset, include, group, having, order, false)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	datas, err := o.db.Query(query)
	if err != nil {
		return nil, err
	}
	return datas, nil
}

func (o *ORM) FindOne(table string, data ...Map) (any, error) {
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

	var queryBuilder SelectQueryBuilder = SelectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.BuildSelectQuery(table, fields, where, limit, -1, include, group, having, order, false)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return nil, nil
}

func (o *ORM) Count(table string, data ...Map) (any, error) {
	if err := o.isValidTableName(table); err != nil {
		return nil, err
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
	}

	var queryBuilder SelectQueryBuilder = SelectQueryBuilder{Schemas: o.schemas}
	query, err := queryBuilder.BuildSelectQuery(table, fields, where, -1, -1, include, group, having, nil, true)
	if err != nil {
		return nil, err
	}
	fmt.Println(query)
	return nil, nil
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

	var findQueryBuilder SelectQueryBuilder = SelectQueryBuilder{Schemas: o.schemas}
	findQuery, findQueryError := findQueryBuilder.BuildSelectQuery(table, fields, where, limit, offset, include, group, having, order, false)
	var countQueryBuilder SelectQueryBuilder = SelectQueryBuilder{Schemas: o.schemas}
	countQuery, countQueryError := countQueryBuilder.BuildSelectQuery(table, fields, where, -1, -1, include, group, having, order, true)
	if findQueryError != nil {
		return nil, findQueryError
	}
	if countQueryError != nil {
		return nil, countQueryError
	}
	fmt.Println(findQuery)
	fmt.Println(countQuery)
	return nil, nil
}
