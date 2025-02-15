package RelisORM

import (
	"fmt"
	"strings"
)

func getWhereCondition(table string, where Where) (string, error) {
	var wheres []string
	if len(where) != 0 {
		for key, value := range where {
			if op, ok := key.(Op); ok {
				ov, err := getOperatorvalue(op, value, table)
				if err != nil {
					return "", err
				}
				wheres = append(wheres, ov)
			} else {
				if value == nil {
					value = "NULL"
				}
				if value == "NULL" {
					wheres = append(wheres, fmt.Sprintf("%v IS %s", parseKey(key, fmt.Sprintf("%s%s", table, key)), value))
				} else {
					w, wOK := value.(Where)
					if _, sOK := key.(string); sOK && wOK {
						val, err := getConditionFromMap(table, w)
						if err != nil {
							return "", err
						}
						wheres = append(wheres, fmt.Sprintf("%s%s %s", table, key, val))
					} else if sl, sOK := value.(SQLLiteral); sOK && wOK {
						val, err := getConditionFromMap(table, w)
						if err != nil {
							return "", err
						}
						wheres = append(wheres, fmt.Sprintf("%s %s", sl.Value, val))
					} else {
						wheres = append(wheres, fmt.Sprintf("%v = %v", parseKey(key, fmt.Sprintf("%s%s", table, key)), parseValue(value, fmt.Sprintf("%v", value))))
					}
				}
			}
		}
	}
	return strings.Join(wheres, " AND "), nil
}

func getConditionFromMap(table string, where Where) (string, error) {
	var l []string
	for key, val := range where {
		if op, oOk := key.(Op); oOk {
			oVal, err := getOperatorvalue(op, val, table)
			if err != nil {
				return "", err
			}
			l = append(l, oVal)
		} else if w, wOK := val.(Where); wOK {
			wVal, err := getConditionFromMap(table, w)
			if err != nil {
				return "", err
			}
			l = append(l, wVal)
		} else {
			if val == nil || val == "NULL" {
				l = append(l, fmt.Sprintf("%v IS NULL", parseKey(key, fmt.Sprintf("%s%v", table, key))))
			} else {
				l = append(l, fmt.Sprintf("%v = %v", parseKey(key, fmt.Sprintf("%s%v", table, key)), parseValue(val, fmt.Sprintf("'%v'", val))))
			}
		}
	}
	return strings.Join(l, " AND "), nil
}

func getOperatorvalue(key Op, value any, table string) (string, error) {
	switch key {
	case Eq:
		return fmt.Sprintf(" = %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Neq:
		return fmt.Sprintf(" != %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Gt:
		return fmt.Sprintf(" > %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Lt:
		return fmt.Sprintf(" < %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Gte:
		return fmt.Sprintf(" >= %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Lte:
		return fmt.Sprintf(" <= %v", parseValue(value, fmt.Sprintf("'%v'", value))), nil
	case Like:
		return fmt.Sprintf(" LIKE %v", parseValue(value, fmt.Sprintf("'%%%v%%'", value))), nil
	case NotLike:
		return fmt.Sprintf(" NOT LIKE %v", parseValue(value, fmt.Sprintf("'%%%v%%'", value))), nil
	case ILike:
		return fmt.Sprintf(" ILIKE %v", parseValue(value, fmt.Sprintf("'%%%v%%'", value))), nil
	case NotILike:
		return fmt.Sprintf(" NOT ILIKE %v", parseValue(value, fmt.Sprintf("'%%%v%%'", value))), nil
	case In:
		v, ok := value.([]any)
		if !ok {
			return "", fmt.Errorf("invalid value for %v : %v", key, value)
		}
		var vSlice []string
		for _, sv := range v {
			vSlice = append(vSlice, fmt.Sprintf("%v", sv))
		}
		return " IN " + strings.Join(vSlice, ","), nil
	case NotIn:
		v, ok := value.([]any)
		if !ok {
			return "", fmt.Errorf("invalid value for %v : %v", key, value)
		}
		var vSlice []string
		for _, sv := range v {
			vSlice = append(vSlice, fmt.Sprintf("%v", sv))
		}
		return " NOT IN " + strings.Join(vSlice, ","), nil
	case Between:
		v, ok := value.([]any)
		if !ok || len(v) != 2 {
			return "", fmt.Errorf("invalid value for %v : %v", key, value)
		}
		return fmt.Sprintf(" BETWEEN %v AND %v", parseValue(v[0], fmt.Sprintf("'%v'", v[0])), parseValue(v[1], fmt.Sprintf("'%v'", v[1]))), nil
	case NotBetween:
		v, ok := value.([]any)
		if !ok || len(v) != 2 {
			return "", fmt.Errorf("invalid value for %v : %v", key, value)
		}
		return fmt.Sprintf(" NOT BETWEEN %v AND %v", parseValue(v[0], fmt.Sprintf("'%v'", v[0])), parseValue(v[1], fmt.Sprintf("'%v'", v[1]))), nil
	case And, Or:
		return getAndOrValue(table, key, value)
	case Not:
		v, ok := value.(Where)
		if !ok {
			return "", fmt.Errorf("invalid value for %v : %v", key, value)
		}
		return getNotvalue(table, key, v)
	}
	return "", nil
}

func getAndOrValue(table string, key Op, value any) (string, error) {
	mVal, mOK := value.(Where)
	lVal, lOK := value.([]any)
	if (!mOK && !lOK) || (mOK && len(mVal) == 0 || lOK && len(lVal) == 0) {
		return "", fmt.Errorf("invalid value for %v : %v", key, value)
	}

	var l []string

	if lOK {
		for _, v := range lVal {
			s, err := getAndOrValue(table, And, v)
			if err != nil {
				return "", err
			}
			l = append(l, s)
		}
	} else if mOK {
		for k, v := range mVal {
			ov, ook := k.(Op)
			wv, mok := v.(Where)

			if ook {
				if ov == And || ov == Or {
					s, err := getAndOrValue(table, ov, v)
					if err != nil {
						return "", err
					}
					l = append(l, s)
				} else if ov == Not {
					s, err := getNotvalue(table, ov, v)
					if err != nil {
						return "", err
					}
					l = append(l, s)
				} else {
					s, err := getOperatorvalue(ov, v, table)
					if err != nil {
						return "", err
					}
					l = append(l, s)
				}
			} else if mok {
				s, err := getConditionFromMap(table, wv)
				if err != nil {
					return "", err
				}

				var firstKeyOp bool = false
				if len(wv) != 0 {
				Inner:
					for fk := range wv {
						if _, o := fk.(Op); o {
							firstKeyOp = true
						}
						break Inner
					}
				}
				if firstKeyOp {
					l = append(l, fmt.Sprintf("%v%s", k, s))
				} else {
					l = append(l, fmt.Sprintf("%v = %s", parseKey(k, fmt.Sprintf("%s%v", table, k)), s))
				}

			} else {
				if v == nil || v == "NULL" {
					l = append(l, fmt.Sprintf("%v IS NULL", parseKey(k, fmt.Sprintf("%s%v", table, k))))
				} else {
					l = append(l, fmt.Sprintf("%v = %v", parseKey(k, fmt.Sprintf("%s%v", table, k)), parseValue(v, fmt.Sprintf("'%v'", v))))
				}
			}
		}
	}

	var s string
	if key == And {
		s = strings.Join(l, " AND ")
	} else {
		s = strings.Join(l, " OR ")
	}

	return "(" + s + ")", nil
}

func getNotvalue(table string, key Op, value any) (string, error) {
	if ov, ok := value.(Op); ok {
		s, err := getOperatorvalue(key, ov, table)
		if err != nil {
			return "", err
		}
		return "NOT (" + s + ")", nil
	}
	mv, ok := value.(Where)
	if !ok {
		return "", fmt.Errorf("invalid value for %v : %v", key, value)
	}

	var l []string

	for k, v := range mv {
		if kov, ook := k.(Op); ook {
			if kov == And || kov == Or {
				s, err := getAndOrValue(table, kov, v)
				if err != nil {
					return "", err
				}
				l = append(l, s)
			} else if kov == Not {
				s, err := getNotvalue(table, kov, v)
				if err != nil {
					return "", err
				}
				l = append(l, s)
			} else {
				s, err := getOperatorvalue(kov, v, table)
				if err != nil {
					return "", err
				}
				l = append(l, s)
			}
		} else {
			if v == nil || v == "NULL" {
				l = append(l, fmt.Sprintf("%v IS NULL", parseKey(k, fmt.Sprintf("%s%v", table, k))))
			} else {
				l = append(l, fmt.Sprintf("%v = %v", parseKey(k, fmt.Sprintf("%s%v", table, k)), parseValue(v, fmt.Sprintf("'%v'", v))))
			}
		}
	}

	return "NOT (" + strings.Join(l, " AND ") + ")", nil
}

func parseKey(obj any, key string) any {
	if sqll, ok := obj.(SQLLiteral); ok {
		return sqll.Value
	}
	return key
}

func parseValue(obj any, value string) any {
	if sqll, ok := obj.(SQLLiteral); ok {
		return sqll.Value
	}
	if !strings.HasPrefix(value, "'") {
		value = "'" + value + "'"
	}
	return value
}
