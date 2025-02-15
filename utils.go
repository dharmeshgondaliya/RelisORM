package RelisORM

func containsKey(m Map, key string) bool {
	_, exists := m[key]
	return exists
}

func containsKeyInMap(m Map, key any) bool {
	if key == nil {
		return false
	}
	s, isString := key.(string)
	if !isString {
		return false
	}
	_, exists := m[s]
	return exists
}
