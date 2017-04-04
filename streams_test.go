package dynamo_test

import (
	"testing"

	apex "github.com/apex/go-apex/dynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/zerops/dynamo"
)

func TestRawEvents(t *testing.T) {
	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	d := []byte("d")

	record := &apex.Record{
		Dynamodb: &apex.StreamRecord{
			NewImage: map[string]*dynamodb.AttributeValue{
				"_4:3": {B: d},
				"_2:1": {B: b},
				"_3:2": {B: c},
				"_1:0": {B: a},
			},
			OldImage: map[string]*dynamodb.AttributeValue{
				"_1:0": {B: a},
			},
		},
	}

	events, err := dynamo.RawEvents(record)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(events), "expected 3 new events")
	assert.Equal(t, b, events[0])
	assert.Equal(t, c, events[1])
	assert.Equal(t, d, events[2])
}

func TestTableName(t *testing.T) {
	arn := "arn:aws:dynamodb:us-west-2:528688496454:table/table-local-orgs/stream/2017-03-14T04:49:34.930"
	tableName, err := dynamo.TableName(arn)
	assert.Nil(t, err)
	assert.Equal(t, "table-local-orgs", tableName)
}
