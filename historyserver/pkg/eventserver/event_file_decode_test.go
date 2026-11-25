package eventserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidEventFile(t *testing.T) {
	cases := []struct {
		name     string
		fileName string
		want     bool
	}{
		{"legacy plain", "node-abcd-2026-05-13-10", true},
		{"legacy gz", "node-abcd-2026-05-13-10.gz", true},
		{"new jsonl", "node-abcd-2026-05-13-10.jsonl", true},
		{"new jsonl gz", "node-abcd-2026-05-13-10.jsonl.gz", true},
		{"nano suffix jsonl.gz", "node-1-2026-05-13-10-1747123200000000000.jsonl.gz", true},
		{"nano suffix jsonl", "node-1-2026-05-13-10-1747123200000000000.jsonl", true},
		{"directory", "some-dir/", false},
		{"unrelated", "foo.txt", false},
		{"wrong hour", "node-abcd-2026-05-13-1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isValidEventFile(tc.fileName))
		})
	}
}

func TestDecodeEventFileBytes_JSONArrayLegacy(t *testing.T) {
	raw := []byte(`[{"eventId":"a","x":1},{"eventId":"b","x":2}]`)
	out, err := DecodeEventFileBytes("node-2026-05-13-10", raw)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "a", out[0]["eventId"])
	assert.Equal(t, "b", out[1]["eventId"])
}

func TestDecodeEventFileBytes_JSONLPlain(t *testing.T) {
	raw := []byte(`{"eventId":"a"}
{"eventId":"b"}

{"eventId":"c"}
`)
	out, err := DecodeEventFileBytes("node-2026-05-13-10.jsonl", raw)
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, "a", out[0]["eventId"])
	assert.Equal(t, "b", out[1]["eventId"])
	assert.Equal(t, "c", out[2]["eventId"])
}

func TestDecodeEventFileBytes_EmptyBytes(t *testing.T) {
	out, err := DecodeEventFileBytes("empty.jsonl", []byte{})
	require.NoError(t, err)
	assert.Nil(t, out)
}

func TestDecodeEventFileBytes_MalformedJSONLLinesSkipped(t *testing.T) {
	raw := []byte(`{"eventId":"a"}
not a json line
{"eventId":"b"}
`)
	out, err := DecodeEventFileBytes("malformed.jsonl", raw)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "a", out[0]["eventId"])
	assert.Equal(t, "b", out[1]["eventId"])
}
