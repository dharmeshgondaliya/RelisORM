package relisorm

type Op int

const (
	Eq Op = iota
	Neq
	Gt
	Gte
	Lt
	Lte
	Between
	NotBetween
	In
	NotIn
	Like
	NotLike
	ILike
	NotILike
	And
	Or
	Not
)
