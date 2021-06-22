package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func (r *testRecord) TaosDatabase() string {
	return ""
}
func (r *testRecord) TaosSTable() string {
	return ""
}
func (r *testRecord) TaosTags() []interface{} {
	var tags []interface{}
	return tags
}
func (r *testRecord) TaosTable() string {
	return r.Name
}
func (r *testRecord) TaosCols() []string {
	var tags []string
	return tags
}
func (r *testRecord) TaosValues() []interface{} {
	var values []interface{}
	values = append(values, r.Name)
	values = append(values, r.Age)
	return values
}

func TestToSql(t *testing.T) {
	raw :=
		testRecord{
			Name: "abc",
			Age:  18,
		}
	sql, err := ToTaosInsertSql(&raw)
	assert.Equal(t, err, nil)
	assert.Equal(t, sql, "insert into abc values('abc',18)")

}
