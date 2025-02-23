package relisorm

type Map map[string]interface{}

type Where map[any]interface{}

type Fields any

type Order map[string]bool

type Schema struct {
	Table  string
	Fields Map
}

type SQLFunction struct {
	Function string
	Column   string
	Alias    string
}

type SQLLiteral struct {
	Value string
}

func (m *Map) ContainsKey(key string) bool {
	_, exists := (*m)[key]
	return exists
}

func (m *Map) ContainsKeyInMap(key any) bool {
	if key == nil {
		return false
	}
	s, isString := key.(string)
	if !isString {
		return false
	}
	_, exists := (*m)[s]
	return exists
}
