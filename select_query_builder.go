package RelisORM

import (
	"fmt"
	"strings"
)

type selectQueryBuilder struct {
	Schemas           map[string]Schema
	responseStructure Map
	responseMapping   map[any]any
	columns           string
	joinString        string
	groupBy           string
	havingCondition   string
	orderBy           string
}

func (s *selectQueryBuilder) buildSelectQuery(table string, fields []Fields, where Where, limit int, offset int, include []Map, group []string, having Where, order Order, isCountQuery bool) (string, error) {
	var fieldList []Fields = s.parseTableFields(table, fields)
	var mainTablePrimaryKey string = s.getPrimaryKeyOfTable(table)
	if mainTablePrimaryKey == "" {
		return "", fmt.Errorf("%s does not contains any primary key", table)
	}
	s.columns = s.getTableFields(table, mainTablePrimaryKey, fieldList, group == nil)

	var groupByFirst string = ""
	if len(group) != 0 {
		groupByFirst = group[0]
		var groupValues []string
		for _, g := range group {
			groupValues = append(groupValues, "\""+table+"\"."+g)
		}
		s.groupBy = "GROUP BY " + strings.Join(groupValues, ", ")
		if len(having) != 0 {
			_condition, err := getWhereCondition("\""+table+"\".", having)
			if err != nil {
				return "", err
			}
			s.havingCondition = _condition
		}
	}

	if len(order) != 0 {
		s.orderBy = "Order By "
		var orderValues []string
		for k, v := range order {
			var ov string
			if v {
				ov = "ASC"
			} else {
				ov = "DESC"
			}
			orderValues = append(orderValues, fmt.Sprintf("\"%s\".%s %s", table, k, ov))
		}
		s.orderBy += strings.Join(orderValues, ", ")
	}

	var responseStructureFields Map = Map{}
	for _, f := range fieldList {
		if fstr, ok := f.(string); ok {
			responseStructureFields[fstr] = table + "." + fstr
		} else if sqlf, ok := f.(SQLFunction); ok {
			responseStructureFields[sqlf.Alias] = table + "." + sqlf.Alias
		}
	}

	s.responseStructure = Map{
		"fields":      responseStructureFields,
		"table":       table,
		"primary_key": mainTablePrimaryKey,
		"mapping_key": table + "." + mainTablePrimaryKey,
	}
	if groupByFirst != "" {
		s.responseStructure["group_key"] = groupByFirst
		s.responseStructure["group_mapping_key"] = table + "." + groupByFirst
	}

	if len(include) != 0 {
		for _, obj := range include {
			if obj == nil {
				return "", fmt.Errorf("syntax is not match for include")
			}
			tableName, ok := obj["table"].(string)
			if !ok || tableName == "" {
				return "", fmt.Errorf("table name cannot be empty")
			}
			if _, ok := s.Schemas[tableName]; !ok {
				return "", fmt.Errorf("%s does not exist", tableName)
			}
			var primaryKey string = s.getPrimaryKeyOfTable(tableName)
			if primaryKey == "" {
				return "", fmt.Errorf("%s does not contains any primary key", tableName)
			}

			var tableFields []Fields
			if obj["fields"] != nil {
				data, ok := obj["fields"].([]Fields)
				if !ok {
					return "", fmt.Errorf("fields type are not match")
				}
				tableFields = data
			}

			var fieldList []Fields = s.parseTableFields(tableName, tableFields)
			s.columns += "," + s.getTableFields(tableName, primaryKey, fieldList, obj["group"] == nil)

			var groupByFirstStr string = ""
			if obj["group"] != nil {
				group, ok := obj["group"].([]string)
				if !ok {
					return "", fmt.Errorf("group type are not match")
				}
				if len(group) != 0 {
					groupByFirstStr = group[0]
					var g []string
					for _, s := range group {
						g = append(g, "\""+tableName+"\"."+s)
					}
					if s.groupBy == "" {
						s.groupBy = "GROUP BY " + strings.Join(g, ", ")
					} else {
						s.groupBy += ", " + strings.Join(g, ", ")
					}

					if obj["having"] != nil {
						havingObj, ok := obj["having"].(Where)
						if !ok {
							return "", fmt.Errorf("having type are not match")
						}
						havingStr, err := getWhereCondition("\""+tableName+"\".", havingObj)
						if err != nil {
							return "", err
						}
						if havingStr != "" {
							if s.havingCondition == "" {
								s.havingCondition = havingStr
							} else {
								s.havingCondition += " AND " + havingStr
							}
						}
					}

				}
			}

			if obj["order"] != nil {
				orders, ok := obj["order"].(Order)
				if !ok {
					return "", fmt.Errorf("order type are not match")
				}
				if len(orders) != 0 {
					if s.orderBy == "" {
						s.orderBy += "Order By "
					} else {
						s.orderBy += ", "
					}
					var orderValues []string
					for k, v := range orders {
						var ov string
						if v {
							ov = "ASC"
						} else {
							ov = "DESC"
						}
						orderValues = append(orderValues, fmt.Sprintf("\"%s\".%s %s", table, k, ov))
					}
					s.orderBy += strings.Join(orderValues, ", ")
				}
			}

			var responseStructureDataFields Map = Map{}
			for _, f := range fieldList {
				if fstr, ok := f.(string); ok {
					responseStructureDataFields[fstr] = table + "." + fstr
				} else if sqlf, ok := f.(SQLFunction); ok {
					responseStructureDataFields[sqlf.Alias] = table + "." + sqlf.Alias
				}
			}

			responseStructureData := Map{
				"fields":      responseStructureDataFields,
				"table":       tableName,
				"primary_key": primaryKey,
				"mapping_key": tableName + "." + primaryKey,
			}
			if groupByFirstStr != "" {
				responseStructureData["group_key"] = groupByFirstStr
				responseStructureData["group_mapping_key"] = tableName + "." + groupByFirstStr
			}
			if s.responseStructure["include"] == nil {
				s.responseStructure["include"] = []Map{}
			}
			if _responseStructureData, ok := s.responseStructure["include"].([]Map); ok {
				s.responseStructure["include"] = append(_responseStructureData, responseStructureData)
			}

			var condition string = ""
			for k, v := range s.Schemas[table].Fields {
				if fieldValue, ok := v.(Map); ok && fieldValue["reference"] != nil {
					reference, referenceOK := fieldValue["reference"].(Map)
					if referenceOK && reference["table"] == tableName {
						condition = fmt.Sprintf("%s.%s=%s.%s", table, reference["column"], tableName, k)
					}
				}
			}
			if condition == "" {
				for k, v := range s.Schemas[tableName].Fields {
					if fieldValue, ok := v.(Map); ok && fieldValue["reference"] != nil {
						reference, referenceOK := fieldValue["reference"].(Map)
						if referenceOK && reference["table"] == table {
							condition = fmt.Sprintf("%s.%s=%s.%s", reference["table"], reference["column"], tableName, k)
						}
					}
				}
			}

			if condition == "" {
				return "", fmt.Errorf("something is wrong with included table")
			}

			var whereCondition string = ""
			var join string = "LEFT JOIN"
			if obj["where"] != nil {
				where, ok := obj["where"].(Where)
				if !ok {
					return "", fmt.Errorf("where type are not match")
				}
				if len(where) != 0 {
					_condition, err := getWhereCondition("\""+tableName+"\".", where)
					if err != nil {
						return "", err
					}
					whereCondition = " AND " + _condition
					join = "INNER JOIN"
				}
			}

			s.joinString += fmt.Sprintf("%s %s AS \"%s\" ON (%s%s)", join, tableName, tableName, condition, whereCondition)

			if obj["include"] != nil {
				includes, ok := obj["include"].([]Map)
				if !ok {
					return "", fmt.Errorf("include type are not match")
				}

				for _, l := range includes {
					if err := s.addIncludeTable(tableName, l, responseStructureData, false); err != nil {
						return "", err
					}
				}
			}
		}
	}

	var limitOffset string = ""
	if limit != -1 {
		limitOffset = fmt.Sprintf("LIMIT %d", limit)
	}
	if offset != -1 {
		limitOffset += fmt.Sprintf(" OFFSET %d", offset)
	}

	var whereCondition string = ""
	if len(where) != 0 {
		_condition, err := getWhereCondition("\""+table+"\".", where)
		if err != nil {
			return "", err
		}
		whereCondition = _condition
	}

	if s.havingCondition != "" {
		s.groupBy += " HAVING " + s.havingCondition
	}

	s.responseMapping = map[any]any{}

	if isCountQuery {
		var selectedColumn string = "*"
		if len(include) != 0 {
			selectedColumn = fmt.Sprintf("\"%s\".%s", table, mainTablePrimaryKey)
		}
		if whereCondition != "" {
			return fmt.Sprintf("SELECT Count(%s) as Count FROM %s AS \"%s\" %s WHERE %s %s", selectedColumn, table, table, s.joinString, whereCondition, s.groupBy), nil
		}
		return fmt.Sprintf("SELECT Count(%s) as Count FROM %s AS \"%s\" %s %s", selectedColumn, table, table, s.joinString, s.groupBy), nil
	}

	var query string = "SELECT " + s.columns + " FROM "
	var subQuery string = table + " AS \"" + table + "\""
	if limitOffset != "" && len(include) != 0 {
		if whereCondition != "" {
			subQuery = fmt.Sprintf("(SELECT * FROM %s WHERE %s %s) AS \"%s\"", table, whereCondition, limitOffset, table)
		} else {
			subQuery = fmt.Sprintf("(SELECT * FROM %s %s) AS \"%s\"", table, limitOffset, table)
		}
		query += subQuery
		if s.joinString != "" {
			query += " " + s.joinString
		}
		if s.groupBy != "" {
			query += " " + s.groupBy
		}
		if s.orderBy != "" {
			query += " " + s.orderBy
		}
	} else {
		query += subQuery
		if whereCondition != "" {
			s.joinString += " WHERE " + whereCondition
		}
		query += " " + s.joinString
		if s.groupBy != "" {
			query += " " + s.groupBy
		}
		if s.orderBy != "" {
			query += " " + s.orderBy
		}
		if limitOffset != "" {
			query += " " + limitOffset
		}
	}

	return query, nil
	// var subQuery string = table + " AS \"" + table + "\""
	// if limitOffset != "" && len(include) == 0 {
	// 	if whereCondition != "" {
	// 		subQuery += " WHERE " + whereCondition
	// 	}
	// 	subQuery += " " + limitOffset
	// } else if limitOffset != "" && whereCondition != "" {
	// 	subQuery = fmt.Sprintf("(SELECT * FROM %s WHERE %s %s) AS \"%s\"", table, whereCondition, limitOffset, table)
	// } else if limitOffset != "" {
	// 	subQuery = fmt.Sprintf("(SELECT * FROM %s %s) AS \"%s\"", table, limitOffset, table)
	// } else if whereCondition != "" {
	// 	s.joinString += " WHERE " + whereCondition
	// }

	// return fmt.Sprintf("SELECT %s FROM %s  %s %s %s", s.columns, subQuery, s.joinString, s.groupBy, s.orderBy), nil
}

func (s *selectQueryBuilder) parseTableFields(table string, fields []Fields) []Fields {
	var tableFields []Fields
	if len(fields) == 0 {
		for k := range s.Schemas[table].Fields {
			tableFields = append(tableFields, k)
		}
		return tableFields
	}
	return fields
}

func (s *selectQueryBuilder) getTableFields(table string, primaryKey string, fields []Fields, canAddPrimaryKey bool) string {
	var primaryKeyIncluded = false
	var columns []string
	for _, field := range fields {
		if s, ok := field.(string); ok {
			columns = append(columns, fmt.Sprintf("\"%s\".%s AS \"%s.%s\"", table, s, table, s))
			if s == primaryKey {
				primaryKeyIncluded = true
			}
		} else if sf, ok := field.(SQLFunction); ok {
			columns = append(columns, fmt.Sprintf("%s(\"%s\".%s) AS \"%s.%s\"", sf.Function, table, sf.Column, table, sf.Alias))
		}
	}
	if !primaryKeyIncluded && canAddPrimaryKey {
		columns = append([]string{fmt.Sprintf("\"%s\".%s AS \"%s.%s\"", table, primaryKey, table, primaryKey)}, columns...)
	}
	return strings.Join(columns, ",")
}

func (s *selectQueryBuilder) getPrimaryKeyOfTable(table string) string {
	for k, v := range s.Schemas[table].Fields {
		if fv, ok := v.(Map); ok && fv["primaryKey"] == true {
			return k
		}
	}
	return ""
}

func (s *selectQueryBuilder) addIncludeTable(tableNames string, includeObj Map, responseStructureData Map, isRecursive bool) error {
	tableSlice := strings.Split(tableNames, "->")
	tableNameTrimmed := tableSlice[len(tableSlice)-1]
	tableName, ok := includeObj["table"].(string)
	if !ok || tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if _, ok := s.Schemas[tableName]; !ok {
		return fmt.Errorf("%s does not exist", tableName)
	}
	var primaryKey string = s.getPrimaryKeyOfTable(tableName)
	if primaryKey == "" {
		return fmt.Errorf("%s does not contains any primary key", tableName)
	}

	var tableFields []Fields
	if includeObj["fields"] != nil {
		data, ok := includeObj["fields"].([]Fields)
		if !ok {
			return fmt.Errorf("fields type are not match")
		}
		tableFields = data
	}

	var fieldList []Fields = s.parseTableFields(tableName, tableFields)
	s.columns += "," + s.getTableFields(tableNames+"->"+tableName, primaryKey, fieldList, includeObj["group"] == nil)

	var groupByFirstStr string = ""
	if includeObj["group"] != nil {
		group, ok := includeObj["group"].([]string)
		if !ok {
			return fmt.Errorf("group type are not match")
		}
		if len(group) != 0 {
			groupByFirstStr = group[0]
			var g []string
			for _, s := range group {
				g = append(g, "\""+tableNames+"->"+tableName+"\"."+s)
			}
			if s.groupBy == "" {
				s.groupBy = "GROUP BY " + strings.Join(g, ", ")
			} else {
				s.groupBy += ", " + strings.Join(g, ", ")
			}

			if includeObj["having"] != nil {
				havingObj, ok := includeObj["having"].(Where)
				if !ok {
					return fmt.Errorf("having type are not match")
				}
				havingStr, err := getWhereCondition("\""+tableNames+"->"+tableName+"\".", havingObj)
				if err != nil {
					return err
				}
				if havingStr != "" {
					if s.havingCondition == "" {
						s.havingCondition = havingStr
					} else {
						s.havingCondition += " AND " + havingStr
					}
				}
			}
		}
	}

	if includeObj["order"] != nil {
		orders, ok := includeObj["order"].(Order)
		if !ok {
			return fmt.Errorf("order type are not match")
		}
		if len(orders) != 0 {
			if s.orderBy == "" {
				s.orderBy += "Order By "
			} else {
				s.orderBy += ", "
			}
			var orderValues []string
			for k, v := range orders {
				var ov string
				if v {
					ov = "ASC"
				} else {
					ov = "DESC"
				}
				orderValues = append(orderValues, fmt.Sprintf("\"%s->%s\".%s %s", tableNames, tableName, k, ov))
			}
			s.orderBy += strings.Join(orderValues, ", ")
		}
	}

	var responseStructureDataFields Map = Map{}
	for _, f := range fieldList {
		if fstr, ok := f.(string); ok {
			responseStructureDataFields[fstr] = tableNames + "->" + tableName + "." + fstr
		} else if sqlf, ok := f.(SQLFunction); ok {
			responseStructureDataFields[sqlf.Alias] = tableNames + "->" + tableName + "." + sqlf.Alias
		}
	}

	responseStructureDatas := Map{
		"fields":      responseStructureDataFields,
		"table":       tableName,
		"primary_key": primaryKey,
		"mapping_key": tableNames + "->" + tableName + "." + primaryKey,
	}
	if groupByFirstStr != "" {
		responseStructureDatas["group_key"] = groupByFirstStr
		responseStructureDatas["group_mapping_key"] = tableNames + "->" + tableName + "." + groupByFirstStr
	}
	if responseStructureData["include"] == nil {
		responseStructureData["include"] = []Map{}
	}
	if _responseStructureData, ok := responseStructureData["include"].([]Map); ok {
		responseStructureData["include"] = append(_responseStructureData, responseStructureDatas)
	}

	var condition string = ""
	for k, v := range s.Schemas[tableNameTrimmed].Fields {
		if fieldValue, ok := v.(Map); ok && fieldValue["reference"] != nil {
			reference, referenceOK := fieldValue["reference"].(Map)
			if referenceOK && reference["table"] == tableName {
				if isRecursive {
					condition = fmt.Sprintf("\"%s\".%s=\"%s->%s\".%s", tableNames, reference["column"], tableNames, tableName, k)
				} else {
					condition = fmt.Sprintf("%s.%s=\"%s->%s\".%s", tableNames, reference["column"], tableNames, tableName, k)
				}
			}
		}
	}
	if condition == "" {
		for k, v := range s.Schemas[tableName].Fields {
			if fieldValue, ok := v.(Map); ok && fieldValue["reference"] != nil {
				reference, referenceOK := fieldValue["reference"].(Map)
				if referenceOK && reference["table"] == tableNameTrimmed {
					if isRecursive {
						condition = fmt.Sprintf("\"%s\".%s=%s.%s", reference["table"], reference["column"], tableNames, k)
					} else {
						condition = fmt.Sprintf("%s.%s=%s.%s", reference["table"], reference["column"], tableNames, k)
					}
				}
			}
		}
	}

	if condition == "" {
		return fmt.Errorf("something is wrong with included table")
	}

	var whereCondition string = ""
	var join string = "LEFT JOIN"
	if includeObj["where"] != nil {
		where, ok := includeObj["where"].(Where)
		if !ok {
			return fmt.Errorf("where type are not match")
		}
		if len(where) != 0 {
			_condition, err := getWhereCondition("\""+tableNames+"->"+tableName+"\".", where)
			if err != nil {
				return err
			}
			whereCondition = " AND " + _condition
			join = "INNER JOIN"
		}
	}

	s.joinString += fmt.Sprintf("%s %s AS \"%s->%s\" ON (%s%s)", join, tableName, tableNames, tableName, condition, whereCondition)

	if includeObj["include"] != nil {
		includes, ok := includeObj["include"].([]Map)
		if !ok {
			return fmt.Errorf("include type are not match")
		}

		for _, l := range includes {
			if err := s.addIncludeTable(tableNames+"->"+tableName, l, responseStructureDatas, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *selectQueryBuilder) parseData(dataList []Map) ([]Map, error) {
	var modifiedData []Map = []Map{}
	var length int = len(dataList)

	for i := range length {
		var data Map = dataList[i]

		var primaryKeyContains bool = false
		var groupMappingContains bool = false
		if containsKeyInMap(data, s.responseStructure["mapping_key"]) {
			_, contains := s.responseMapping[data[s.responseStructure["mapping_key"].(string)]]
			primaryKeyContains = !contains
		}
		if containsKey(s.responseStructure, "group_mapping_key") {
			if !containsKeyInMap(data, s.responseStructure["group_mapping_key"]) {
				groupMappingContains = true
			}
		}

		if primaryKeyContains || groupMappingContains {
			s.responseMapping[data[s.responseStructure["mapping_key"].(string)]] = map[any]any{"index": len(modifiedData)}
			modifiedData = append(modifiedData, make(Map))

			d, isMap := s.responseMapping[data[s.responseStructure["mapping_key"].(string)]].(map[any]any)
			if isMap {
				index, isInt := d["index"].(int)
				if isInt {
					var modifiedMapData Map = modifiedData[index]

					if fields, fieldsOK := s.responseStructure["fields"].(Map); fieldsOK {
						for k, v := range fields {
							modifiedMapData[k] = data[v.(string)]
						}
					}
				}

			}

		}

		if s.responseStructure["include"] != nil {
			if includeStructures, includeOK := s.responseStructure["include"].([]Map); includeOK {
				for _, includeStructure := range includeStructures {
					d, isMap := s.responseMapping[data[s.responseStructure["mapping_key"].(string)]].(map[any]any)
					if isMap {
						index, isInt := d["index"].(int)
						if isInt {
							var modifiedMapData Map = modifiedData[index]
							s.parseIncludeData(data, modifiedMapData, includeStructure, d)
						}

					}
				}
			}
		}

	}

	return modifiedData, nil
}

func (s *selectQueryBuilder) parseIncludeData(data Map, modifiedData Map, responseStructureData Map, responseMappingData map[any]any) error {
	if modifiedData[responseStructureData["table"].(string)] == nil {
		modifiedData[responseStructureData["table"].(string)] = []Map{}
	}
	if responseMappingData[responseStructureData["table"]] == nil {
		responseMappingData[responseStructureData["table"]] = map[any]any{}
	}

	_, primayKeyContains := responseMappingData[responseStructureData["table"]].(map[any]any)[data[responseStructureData["mapping_key"].(string)]]
	if !primayKeyContains {
		responseMappingData[responseStructureData["table"]].(map[any]any)[data[responseStructureData["mapping_key"].(string)]] = map[any]any{"index": len(modifiedData[responseStructureData["table"].(string)].([]Map))}
		modifiedData[responseStructureData["table"].(string)] = append(modifiedData[responseStructureData["table"].(string)].([]Map), make(Map))
		var modifiedMapData Map = modifiedData[responseStructureData["table"].(string)].([]Map)[responseMappingData[responseStructureData["table"].(string)].(map[any]any)[data[responseStructureData["mapping_key"].(string)]].(map[any]any)["index"].(int)]
		for k, v := range responseStructureData["fields"].(Map) {
			modifiedMapData[k] = data[v.(string)]
		}
	}

	if responseStructureData["include"] != nil {
		if includeStructures, includeOK := responseStructureData["include"].([]Map); includeOK {
			for _, includeStructure := range includeStructures {
				var modifiedMapData Map = modifiedData[responseStructureData["table"].(string)].([]Map)[responseMappingData[responseStructureData["table"].(string)].(map[any]any)[data[responseStructureData["mapping_key"].(string)]].(map[any]any)["index"].(int)]
				return s.parseIncludeData(data, modifiedMapData, includeStructure, responseMappingData[responseStructureData["table"].(string)].(map[any]any)[data[responseStructureData["mapping_key"].(string)]].(map[any]any))
			}
		}
	}

	return nil
}
