package dynamo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zerops/dynamo"
)

func TestMakeCreateTableInput(t *testing.T) {
	expected := "new-hash-key"
	input := dynamo.MakeCreateTableInput("blah", 3, 3, dynamo.WithHashKey(expected))
	assert.Equal(t, expected, *input.AttributeDefinitions[0].AttributeName)
	assert.Equal(t, expected, *input.KeySchema[0].AttributeName)
}

func TestWithStreams(t *testing.T) {
	input := dynamo.MakeCreateTableInput("blah", 3, 3, dynamo.WithStreams())
	assert.NotNil(t, input.StreamSpecification)
	assert.True(t, *input.StreamSpecification.StreamEnabled)
	assert.Equal(t, "NEW_AND_OLD_IMAGES", *input.StreamSpecification.StreamViewType)
}
