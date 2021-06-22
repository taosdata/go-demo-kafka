package record

import (
	"time"

	"github.com/taosdata/go-demo-kafka/pkg/utils"
)

type Record struct {
	DeviceID  string // for table name
	Timestamp time.Time
	Cols      []interface{}
}

// TDengineEncoder methods

// If this is setted, sql will use db.table for tablename
func (r Record) TaosDatabase() string {
	return ""
}

// Auto create table using stable and tags
func (r Record) TaosSTable() string {
	return "stb"
}

// tags must be setted with TaosSTable
func (r Record) TaosTags() []interface{} {
	var tags []interface{}
	tags = append(tags, r.DeviceID)
	return tags
}

// Dynamic device id as table name
func (r Record) TaosTable() string {
	return r.DeviceID
}

// Use specific column names as you need
func (r Record) TaosCols() []string {
	var tags []string
	return tags
}

// Values
func (r Record) TaosValues() []interface{} {
	var values []interface{}
	values = append(values, r.Timestamp)
	values = append(values, r.Cols...)
	return values
}

func (r Record) CodecMethod() utils.CodecMethodEnum {
	return utils.MessagePack
}
