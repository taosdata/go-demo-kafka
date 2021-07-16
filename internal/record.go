package record

import (
	"hash/fnv"
	"time"

	"github.com/taosdata/go-demo-kafka/pkg/utils"
)

// Record is the structual message object detail, provide both Codec and TaosEncoder interface.
type Record struct {
	DeviceID  string        `json:"deviceID"` // for table name
	Timestamp time.Time     `json:"timestamp"`
	Cols      []interface{} `json:"cols"`
}

// Change max partitions as you need.
const MAX_PARTITIONS = 10

// TaosEncoder implementations

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

// Codec interface

// Encoding method
func (r Record) CodecMethod() utils.CodecMethodEnum {
	return utils.MessagePack
}

// How to set partition for an message
func (r Record) Partition() int32 {
	h := fnv.New32a()
	h.Write([]byte(r.DeviceID))
	return int32(h.Sum32() % MAX_PARTITIONS)
}
