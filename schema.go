package RelisORM

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
