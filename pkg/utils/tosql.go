package utils

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNoTagsFound   = errors.New("there's no tags found for the record")
	ErrNoValuesFound = errors.New("there's no values found for the record")
	ErrNoColsDefined = errors.New("no col names defined")
)

type TaosEncoder interface {
	TaosDatabase() string
	TaosSTable() string
	TaosTable() string
	TaosTags() []interface{}
	TaosCols() []string
	TaosValues() []interface{}
}

/// Array to sql for TDengine
func arrayToSqlString(v []interface{}) string {
	if len(v) == 0 {
		return ""
	}
	tt := make([]string, len(v))
	for i, tag := range v {
		switch vv := tag.(type) {
		case nil:
			tt[i] = "NULL"
		case time.Time:
			tt[i] = fmt.Sprintf("%d", vv.UnixNano()/(int64(time.Millisecond)/int64(time.Nanosecond)))
		case string:
			tt[i] = fmt.Sprintf("'%s'", vv)
		default:
			tt[i] = fmt.Sprint(vv)
		}
	}
	return strings.Join(tt, ",")
}

// interface array to tags array
func toTagsSql(t TaosEncoder) (string, error) {
	tags := t.TaosTags()
	if len(tags) == 0 {
		return "", ErrNoTagsFound
	}
	return arrayToSqlString(tags), nil
}

// interface array to tags array
func toValuesSql(t TaosEncoder) (string, error) {
	v := t.TaosValues()
	if len(v) == 0 {
		return "", ErrNoValuesFound
	}
	return arrayToSqlString(v), nil
}
func toColsSql(t TaosEncoder) (string, error) {
	cols := t.TaosCols()
	if len(cols) == 0 {
		return "", ErrNoColsDefined
	}
	return strings.Join(cols, ","), nil
}
func ToTaosRecordSql(t TaosEncoder) (string, error) {
	db := t.TaosDatabase()
	var sql []string
	var table string
	if db != "" {
		table = fmt.Sprintf("%s.%s", db, t.TaosTable())
	} else {
		table = t.TaosTable()
	}
	sql = append(sql, table)
	stable := t.TaosSTable()
	if stable != "" {
		if db != "" {
			sql = append(sql, fmt.Sprintf("using %s.%s", db, stable))
		} else {
			sql = append(sql, fmt.Sprintf("using %s", stable))
		}
	}
	tags, tagsErr := toTagsSql(t)
	if tagsErr == nil {
		sql = append(sql, fmt.Sprintf("tags(%s)", tags))
	}

	cols, colsErr := toColsSql(t)
	if colsErr == nil {
		sql = append(sql, cols)
	}

	values, valuesErr := toValuesSql(t)
	if valuesErr != nil {
		return "", valuesErr
	}
	sql = append(sql, fmt.Sprintf("values(%s)", values))
	return strings.Join(sql, " "), nil
}

func ToTaosInsertSql(t TaosEncoder) (string, error) {
	record, err := ToTaosRecordSql(t)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("insert into %s", record), nil
}

// batch records to TDengine sql
func ToTaosBatchInsertSql(l []interface{}) (string, error) {
	records := make([]string, len(l))
	for i, t := range l {
		// type TaosEncoderT = *TaosEncoder
		record, err := ToTaosRecordSql(t.(TaosEncoder))
		if err != nil {
			return "", err
		}
		records[i] = record
	}
	return fmt.Sprintf("insert into %s", strings.Join(records, " ")), nil
}
