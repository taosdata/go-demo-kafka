package record

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToSql(t *testing.T) {
	c := DefaultConfig()

	c.Columns = 2
	c.Tables = 2
	c.RecordsPerTable = 10
	recordsTotal := 0
	for record := range c.Records() {
		recordsTotal++
		fmt.Println(record)
	}
	assert.Equal(t, recordsTotal, c.Tables*c.RecordsPerTable)

}
