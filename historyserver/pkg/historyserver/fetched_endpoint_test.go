package historyserver

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMissingSnapshotFallback(t *testing.T) {
	emptyPlacementGroups := map[string]any{
		"result": true,
		"msg":    "",
		"data": map[string]any{
			"result": map[string]any{
				"total":                   float64(0),
				"num_after_truncation":    float64(0),
				"num_filtered":            float64(0),
				"result":                  []any{},
				"partial_failure_warning": "",
				"warnings":                nil,
			},
		},
	}

	tests := []struct {
		name string
		path string
		want any
	}{
		{
			name: "Serve",
			path: "/api/serve/applications/",
			want: map[string]any{"applications": map[string]any{}},
		},
		{
			name: "Ray Data",
			path: "/api/data/datasets/01000000",
			want: map[string]any{"datasets": []any{}},
		},
		{
			name: "placement groups",
			path: "/api/v0/placement_groups",
			want: emptyPlacementGroups,
		},
		{
			name: "placement groups trailing slash",
			path: "/api/v0/placement_groups/",
			want: emptyPlacementGroups,
		},
		{
			name: "placement groups extra path segment",
			path: "/api/v0/placement_groups/unknown",
		},
		{
			name: "unknown",
			path: "/api/unknown",
		},
		{
			name: "Ray Data missing job ID",
			path: "/api/data/datasets/",
		},
		{
			name: "Ray Data extra path segment",
			path: "/api/data/datasets/01000000/unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := missingSnapshotFallback(test.path)
			if test.want == nil {
				if body != nil {
					t.Fatalf("missingSnapshotFallback(%q) = %s, want nil", test.path, body)
				}
				return
			}

			var got any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("missingSnapshotFallback(%q) returned invalid JSON: %v", test.path, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("missingSnapshotFallback(%q) = %#v, want %#v", test.path, got, test.want)
			}
		})
	}
}

func TestEnsurePlacementGroupBundles(t *testing.T) {
	t.Run("backfills only missing bundles", func(t *testing.T) {
		body := []byte(`{"data":{"result":{"result":[{"placement_group_id":"pg-1"}]}}}`)
		patched := decodePlacementGroupResponse(t, ensurePlacementGroupBundles(body))
		group := placementGroupAt(t, patched, 0)

		if bundles, ok := group["bundles"].([]any); !ok || len(bundles) != 0 {
			t.Fatalf("bundles = %#v, want empty array", group["bundles"])
		}
		if _, exists := group["stats"]; exists {
			t.Fatalf("stats = %#v, want field to remain absent", group["stats"])
		}
	})

	t.Run("returns a complete payload byte-for-byte", func(t *testing.T) {
		body := []byte(`{ "data": {"result":{"result":[{"placement_group_id":"<pg-1>","bundles":[{"unit_resources":{"CPU":1}}],"stats":{"scheduling_state":"FINISHED"}}]}}}`)
		if got := ensurePlacementGroupBundles(body); string(got) != string(body) {
			t.Fatalf("ensurePlacementGroupBundles() = %q, want original %q", got, body)
		}
	})

	t.Run("leaves invalid JSON unchanged", func(t *testing.T) {
		body := []byte(`not-json`)
		if got := ensurePlacementGroupBundles(body); string(got) != string(body) {
			t.Fatalf("ensurePlacementGroupBundles() = %q, want %q", got, body)
		}
	})
}

func decodePlacementGroupResponse(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("failed to decode placement group response: %v", err)
	}
	return response
}

func placementGroupAt(t *testing.T, response map[string]any, index int) map[string]any {
	t.Helper()
	data := response["data"].(map[string]any)
	result := data["result"].(map[string]any)
	groups := result["result"].([]any)
	return groups[index].(map[string]any)
}
