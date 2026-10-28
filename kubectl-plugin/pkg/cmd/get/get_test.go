package get

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoinLabelMap(t *testing.T) {
	labels := map[string]string{
		"lain":   "holsten",
		"portia": "fabian",
	}
	output := joinLabelMap(labels)
	parts := strings.Split(output, ",")
	assert.Contains(t, parts, "lain=holsten")
	assert.Contains(t, parts, "portia=fabian")
}
