package record

import (
	"fmt"
	"math/rand"
	"time"
)

type Config struct {
	Database        string
	STable          string
	TablePrefix     string
	Tables          int
	RecordsPerTable int
	Columns         int
	BytesPerColumn  int
	RandSeed        int
	StartTs         time.Time
	RecordInterval  time.Duration
}

var GlobalConfig Config

// var rand *rand.Rand

func DefaultConfig() *Config {
	GlobalConfig = Config{
		Database:        "",
		STable:          "stb",
		TablePrefix:     "tb",
		Tables:          10,
		RecordsPerTable: 10,
		Columns:         10,
		BytesPerColumn:  10,
		RandSeed:        99,
		StartTs:         time.Now(),
		RecordInterval:  1 * time.Second,
	}
	return &GlobalConfig
}

func (c *Config) FlagInit() {

}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (c *Config) randomRecord(t int, r int) Record {
	var record Record
	record.DeviceID = fmt.Sprintf("%s%d", c.TablePrefix, t)
	dur, _ := time.ParseDuration(fmt.Sprintf("%dns", int(c.RecordInterval.Nanoseconds())*r))
	record.Timestamp = c.StartTs.Add(dur)

	for i := 0; i < c.Columns; i++ {
		record.Cols = append(record.Cols, RandomString(c.BytesPerColumn))
	}
	return record
}

func (c *Config) Records() chan Record {
	ch := make(chan Record)
	go func() {
		for r := 0; r < c.RecordsPerTable; r++ {
			for t := 0; t < c.Tables; t++ {
				ch <- c.randomRecord(t, r)
			}
		}
		close(ch)
	}()
	return ch
}

func (c *Config) tableName() string {
	if c.Database == "" {
		return c.STable
	} else {
		return fmt.Sprintf("%s.%s", c.Database, c.STable)
	}
}

func (c *Config) CreateTableSql() string {
	values := "ts timestamp"
	for i := 0; i < c.Columns; i++ {
		values = fmt.Sprintf("%s,col%d binary(%d)", values, i, c.BytesPerColumn)
	}
	sql := fmt.Sprintf("create stable if not exists %s (%s) tags(device binary(10))", c.tableName(), values)
	return sql
}

func (c *Config) DropTableSql() string {
	sql := fmt.Sprintf("drop stable if exists %s", c.tableName())
	return sql
}
