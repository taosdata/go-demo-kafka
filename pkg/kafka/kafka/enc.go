package kafka

//encoder

import (
	"bytes"

	broker "github.com/taosdata/go-demo-kafka/pkg/kafka/common"
	"github.com/taosdata/go-demo-kafka/pkg/utils/json"

	"github.com/Shopify/sarama"

	"reflect"
	"strings"
)

func argInfo(cb broker.Handler) (reflect.Type, int) {
	cbType := reflect.TypeOf(cb)
	if cbType.Kind() != reflect.Func {
		panic("kafka: Handler needs to be a func")
	}
	numArgs := cbType.NumIn()
	if numArgs == 0 {
		return nil, numArgs
	}
	return cbType.In(numArgs - 1), numArgs
}

func kafkaCB(wantsRaw bool, argType reflect.Type, numArgs int, cbValue reflect.Value) func(m *sarama.ConsumerMessage) error {
	return func(m *sarama.ConsumerMessage) (err error) {
		var oV []reflect.Value
		if wantsRaw {
			oV = []reflect.Value{reflect.ValueOf(m)}
		} else {
			var oPtr reflect.Value
			if argType.Kind() != reflect.Ptr {
				oPtr = reflect.New(argType)
			} else {
				oPtr = reflect.New(argType.Elem())
			}
			if err = jsonEncoder.Decode(m.Value, oPtr.Interface()); err != nil {
				return err
			}
			if argType.Kind() != reflect.Ptr {
				oPtr = reflect.Indirect(oPtr)
			}

			// Callback Arity
			switch numArgs {
			case 1:
				oV = []reflect.Value{oPtr}
			case 2:
				subV := reflect.ValueOf(m.Topic)
				oV = []reflect.Value{subV, oPtr}
			case 3:
				subV := reflect.ValueOf(m.Topic)
				keyV := reflect.ValueOf(string(m.Key))
				oV = []reflect.Value{subV, keyV, oPtr}
			case 4:
				subV := reflect.ValueOf(m.Topic)
				keyV := reflect.ValueOf(string(m.Key))
				headers := make(map[string]string)
				for _, header := range m.Headers {
					headers[string(header.Key)] = string(header.Value)
				}
				headersV := reflect.ValueOf(headers)
				oV = []reflect.Value{subV, keyV, headersV, oPtr}
			}
		}
		cbValue.Call(oV)
		return err
	}
}

var jsonEncoder JsonEncoder

// JsonEncoder is a JSON Encoder implementation for EncodedConn.
type JsonEncoder struct {
	// Empty
}

// Encode
func (je *JsonEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Decode
func (je *JsonEncoder) Decode(data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		// If they want a string and it is a JSON string, strip quotes
		// This allows someone to send a struct but receive as a plain string
		// This cast should be efficient for Go 1.3 and beyond.
		str := string(data)
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		err = decoder.Decode(arg)
		//err = json.Unmarshal(data, arg)
	}
	return
}
