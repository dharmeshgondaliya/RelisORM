package relisorm

import (
	"fmt"
	"strings"
)

func createTableQueries(schemas map[string]Schema) ([]string, error) {
	var tables []string
	for table, _schema := range schemas {
		var fields []string
		for key, value := range _schema.Fields {

			if dataType, ok := value.(DataType); ok {
				fields = append(fields, key+" "+dataType.Value)
			} else if mapvalue, ok := value.(Map); ok {
				if mapvalue["type"] == nil {
					return nil, fmt.Errorf("datatype is missing for %s", key)
				} else if dataType, ok := mapvalue["type"].(DataType); ok {
					var column string = key + " " + dataType.Value
					if mapvalue["primaryKey"] == true {
						column += " PRIMARY KEY"
					}
					if mapvalue["autoIncrement"] == true {
						column += " AUTOINCREMENT"
					}
					if mapvalue["auto_increment"] == true {
						column += " AUTO_INCREMENT"
					}
					if mapvalue["allowNull"] == false {
						column += " NOT NULL"
					}
					if mapvalue["default"] != nil {
						column += fmt.Sprintf(" DEFAULT %v", mapvalue["default"])
					}
					if mapvalue["unique"] == true {
						column += " UNIQUE"
					}

					if mapvalue["reference"] != nil {
						if mapVal, ok := mapvalue["reference"].(Map); ok {
							if mapVal["table"] == nil || mapVal["column"] == nil {
								return nil, fmt.Errorf("invalid reference for %s", key)
							}
							if table, ValidTableName := mapVal["table"].(string); !ValidTableName {
								return nil, fmt.Errorf("invalid table name: %v", table)
							}
							if column, validColumnName := mapVal["column"].(string); !validColumnName {
								return nil, fmt.Errorf("invalid table name: %v", column)
							}

							column += fmt.Sprintf(" REFERENCES %s(%s)", mapVal["table"], mapVal["column"])
							if mapvalue["cascase"] == true {
								column += " ON DELETE CASCADE"
							}
							if mapvalue["restrict"] == true {
								column += " ON DELETE RESTRICT"
							}
							if mapvalue["setNull"] == true {
								column += " ON DELETE SET NULL"
							}

						} else {
							return nil, fmt.Errorf("invalid reference for %s", key)
						}
					}

					fields = append(fields, column)
				} else {
					return nil, fmt.Errorf("invalid data type of %s: %v", key, mapvalue["type"])
				}
			} else {
				return nil, fmt.Errorf("invalid data type of %s: %s", key, value)
			}
		}
		var sqlQuery string = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s)", table, strings.Join(fields, ","))
		tables = append(tables, sqlQuery)
	}
	return tables, nil
}

func deleteAllTablesQuery() string {
	return `
		DO $$
		DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE') LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || r.table_name || ' CASCADE;';
			END LOOP;
		END $$;
		`
}
