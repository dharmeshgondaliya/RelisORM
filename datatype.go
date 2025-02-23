package relisorm

import "fmt"

type DataType struct{ Value string }

var DataTypes = struct {
	BINARYSTRING func() DataType
	TEXT         func() DataType
	TINYTEXT     func() DataType
	CITEXT       func() DataType
	TSVECTOR     func() DataType
	BOOLEAN      func() DataType
	INTEGER      func() DataType
	SMALLINT     func() DataType
	INTEGERARRAY func() DataType
	DATE         func() DataType
	TIME         func() DataType
	UUID         func() DataType
	BYTEA        func() DataType
	SERIAL       func() DataType
	BIGSERIAL    func() DataType
	BIGINT       func(length ...int) DataType
	STRING       func(length ...int) DataType
	VARCHARARRAY func(length ...int) DataType
	FLOAT        func(length ...int) DataType
	DOUBLE       func(length ...int) DataType
	DECIMAL      func(length ...int) DataType
	REAL         func(length ...int) DataType
	NUMERIC      func(length ...int) DataType
	VALUE        func(Value string) DataType
}{
	BINARYSTRING: func() DataType {
		return DataType{Value: "VARCHAR BINARY"}
	},
	TEXT: func() DataType {
		return DataType{Value: "TEXT"}
	},
	TINYTEXT: func() DataType {
		return DataType{Value: "TINYTEXT"}
	},
	CITEXT: func() DataType {
		return DataType{Value: "CITEXT"}
	},
	TSVECTOR: func() DataType {
		return DataType{Value: "TSVECTOR"}
	},
	BOOLEAN: func() DataType {
		return DataType{Value: "BOOLEAN"}
	},
	INTEGER: func() DataType {
		return DataType{Value: "INTEGER"}
	},
	SMALLINT: func() DataType {
		return DataType{Value: "SMALLINT"}
	},
	INTEGERARRAY: func() DataType {
		return DataType{Value: "INTEGER[]"}
	},
	DATE: func() DataType {
		return DataType{Value: "DATE"}
	},
	TIME: func() DataType {
		return DataType{Value: "TIME"}
	},
	UUID: func() DataType {
		return DataType{Value: "UUID"}
	},
	BYTEA: func() DataType {
		return DataType{Value: "BYTEA"}
	},
	SERIAL: func() DataType {
		return DataType{Value: "SERIAL"}
	},
	BIGSERIAL: func() DataType {
		return DataType{Value: "BIGSERIAL"}
	},
	STRING: func(length ...int) DataType {
		if len(length) == 1 {
			return DataType{Value: fmt.Sprintf("VARCHAR(%d)", length[0])}
		}
		return DataType{Value: fmt.Sprintf("VARCHAR(%d)", 255)}
	},
	BIGINT: func(length ...int) DataType {
		if len(length) == 1 {
			return DataType{Value: fmt.Sprintf("BIGINT(%d)", length[0])}
		}
		return DataType{Value: "BIGINT"}
	},
	VARCHARARRAY: func(length ...int) DataType {
		if len(length) == 1 {
			return DataType{Value: fmt.Sprintf("VARCHAR(%d)[]", length[0])}
		}
		return DataType{Value: "VARCHAR(100)[]"}
	},
	FLOAT: func(length ...int) DataType {
		var arrayLength int = len(length)
		if arrayLength == 2 {
			return DataType{Value: fmt.Sprintf("FLOAT(%d,%d)", length[0], length[1])}
		}
		if arrayLength == 1 {
			return DataType{Value: fmt.Sprintf("FLOAT(%d)", length[0])}
		}
		return DataType{Value: "FLOAT"}
	},
	DOUBLE: func(length ...int) DataType {
		var arrayLength int = len(length)
		if arrayLength == 2 {
			return DataType{Value: fmt.Sprintf("DOUBLE(%d,%d)", length[0], length[1])}
		}
		if arrayLength == 1 {
			return DataType{Value: fmt.Sprintf("DOUBLE(%d)", length[0])}
		}
		return DataType{Value: "DOUBLE"}
	},
	DECIMAL: func(length ...int) DataType {
		var arrayLength int = len(length)
		if arrayLength == 2 {
			return DataType{Value: fmt.Sprintf("DECIMAL(%d,%d)", length[0], length[1])}
		}
		if arrayLength == 1 {
			return DataType{Value: fmt.Sprintf("DECIMAL(%d)", length[0])}
		}
		return DataType{Value: "DECIMAL"}
	},
	REAL: func(length ...int) DataType {
		var arrayLength int = len(length)
		if arrayLength == 2 {
			return DataType{Value: fmt.Sprintf("REAL(%d,%d)", length[0], length[1])}
		}
		if arrayLength == 1 {
			return DataType{Value: fmt.Sprintf("REAL(%d)", length[0])}
		}
		return DataType{Value: "REAL"}
	},
	NUMERIC: func(length ...int) DataType {
		var arrayLength int = len(length)
		if arrayLength == 2 {
			return DataType{Value: fmt.Sprintf("NUMERIC(%d,%d)", length[0], length[1])}
		}
		if arrayLength == 1 {
			return DataType{Value: fmt.Sprintf("NUMERIC(%d)", length[0])}
		}
		return DataType{Value: "NUMERIC"}
	},
	VALUE: func(Value string) DataType {
		return DataType{Value: Value}
	},
}
