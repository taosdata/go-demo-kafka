package utils

import (
	"encoding/json"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

var ErrUnsupportCodec = errors.New("Unsupported codec")

type CodecMethodEnum int

const (
	Json CodecMethodEnum = iota
	MessagePack
)

type Codec interface {
	CodecMethod() CodecMethodEnum
}

func ToKafkaBytes(k Codec) ([]byte, error) {
	switch k.CodecMethod() {
	case Json:
		return json.Marshal(k)
	case MessagePack:
		return msgpack.Marshal(k)
	default:
		return nil, ErrUnsupportCodec
	}
}

func FromKafkaBytes(bytes []byte, record Codec) error {
	switch record.CodecMethod() {
	case Json:
		return json.Unmarshal(bytes, record)
	case MessagePack:
		return msgpack.Unmarshal(bytes, record)
	default:
		return ErrUnsupportCodec
	}
}
