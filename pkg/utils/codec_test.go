package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRecord struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (*testRecord) CodecMethod() CodecMethodEnum {
	return Json
}
func (*testRecord) Partition() int32 {
	return -1
}
func TestCodec(t *testing.T) {
	raw :=
		testRecord{
			Name: "abc",
			Age:  18,
		}
	bytes, err := ToKafkaBytes(&raw)
	s := string(bytes)
	fmt.Println(s)
	assert.True(t, err == nil)

	var r testRecord
	FromKafkaBytes(bytes, &r)
	assert.Equal(t, raw, r)

}
