package clustermetadata

import (
	"testing"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func TestDecodePath(t *testing.T) {
	tests := []struct {
		name      string
		filePath  string
		rootDir   string
		expectErr bool
		expected  utils.ClusterInfo
	}{
		{
			name:      "valid hierarchical path job",
			filePath:  "cluster-metadata/rayjob/defaultns_myrayjob_mycluster3/session_2024-05-15_10-30-55_123456",
			expectErr: false,
			expected: utils.ClusterInfo{
				Namespace:       "defaultns",
				OwnerKind:       "rayjob",
				OwnerName:       "myrayjob",
				Name:            "mycluster3",
				SessionName:     "session_2024-05-15_10-30-55_123456",
				CreateTimeStamp: time.Date(2024, time.May, 15, 10, 30, 55, 123456000, time.UTC).Unix(),
				CreateTime:      "2024-05-15T10:30:55Z",
			},
		},
		{
			name:      "valid hierarchical path standalone",
			filePath:  "cluster-metadata/raycluster/defaultns_mycluster1/session_2024-05-15_10-30-55_123456",
			expectErr: false,
			expected: utils.ClusterInfo{
				Namespace:       "defaultns",
				Name:            "mycluster1",
				SessionName:     "session_2024-05-15_10-30-55_123456",
				CreateTimeStamp: time.Date(2024, time.May, 15, 10, 30, 55, 123456000, time.UTC).Unix(),
				CreateTime:      "2024-05-15T10:30:55Z",
			},
		},
		{
			name:      "invalid path prefix",
			filePath:  "wrong-prefix/rayjob/defaultns_myrayjob_mycluster3/session_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
		{
			name:      "invalid path prefix with custom rootDir",
			filePath:  "cluster-metadata/rayjob/defaultns_myrayjob_mycluster3/session_2024-05-15_10-30-55_123456",
			rootDir:   "custom-root",
			expectErr: true,
		},
		{
			name:      "invalid path structure - missing session",
			filePath:  "cluster-metadata/rayjob/defaultns_myrayjob_mycluster3",
			expectErr: true,
		},
		{
			name:      "invalid path structure - too many parts",
			filePath:  "cluster-metadata/rayjob/defaultns_myrayjob_mycluster3/session/extra",
			expectErr: true,
		},
		{
			name:      "invalid metadir segment - only one part",
			filePath:  "cluster-metadata/raycluster/defaultns/session_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
		{
			name:      "invalid metadir segment with owner - too few parts",
			filePath:  "cluster-metadata/rayjob/defaultns/session_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
		{
			name:      "session id with bad timestamp pattern",
			filePath:  "cluster-metadata/raycluster/defaultns_mycluster/session_not-a-real-timestamp",
			expectErr: true,
		},
		{
			name:      "invalid decode - raycluster with 3 meta parts",
			filePath:  "cluster-metadata/raycluster/defaultns_myservice_mycluster3/session_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
		{
			name:      "valid owner kind - mixed case",
			filePath:  "cluster-metadata/RaYSerVice/defaultns_myservice_mycluster3/session_2024-05-15_10-30-55_123456",
			expectErr: false,
			expected: utils.ClusterInfo{
				Namespace:       "defaultns",
				OwnerKind:       "rayservice",
				OwnerName:       "myservice",
				Name:            "mycluster3",
				SessionName:     "session_2024-05-15_10-30-55_123456",
				CreateTimeStamp: time.Date(2024, time.May, 15, 10, 30, 55, 123456000, time.UTC).Unix(),
				CreateTime:      "2024-05-15T10:30:55Z",
			},
		},
		{
			name:      "unsupported owner kind",
			filePath:  "cluster-metadata/wrongowner/defaultns_myservice_name/session_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := DecodePath(tc.filePath, tc.rootDir)

			if tc.expectErr {
				if err == nil {
					t.Errorf("DecodePath(%q) succeeded unexpectedly", tc.filePath)
				}
			} else {
				if err != nil {
					t.Fatalf("DecodePath(%q) failed unexpectedly: %v", tc.filePath, err)
				}
				if c.Namespace != tc.expected.Namespace {
					t.Errorf("Namespace = %q, want %q", c.Namespace, tc.expected.Namespace)
				}
				if c.Name != tc.expected.Name {
					t.Errorf("Name = %q, want %q", c.Name, tc.expected.Name)
				}
				if c.SessionName != tc.expected.SessionName {
					t.Errorf("SessionName = %q, want %q", c.SessionName, tc.expected.SessionName)
				}
				if c.OwnerKind != tc.expected.OwnerKind {
					t.Errorf("OwnerKind = %q, want %q", c.OwnerKind, tc.expected.OwnerKind)
				}
				if c.OwnerName != tc.expected.OwnerName {
					t.Errorf("OwnerName = %q, want %q", c.OwnerName, tc.expected.OwnerName)
				}
				if c.CreateTimeStamp != tc.expected.CreateTimeStamp {
					t.Errorf("CreateTimeStamp = %d, want %d", c.CreateTimeStamp, tc.expected.CreateTimeStamp)
				}
				if c.CreateTime != tc.expected.CreateTime {
					t.Errorf("CreateTime = %q, want %q", c.CreateTime, tc.expected.CreateTime)
				}
			}
		})
	}
}

func TestMetadirPathRoundTrip(t *testing.T) {
	const sessionID = "session_2026-05-15_10-30-55_123456"
	sessionTime, _ := utils.GetDateTimeFromSessionID(sessionID)

	cases := []utils.ClusterInfo{
		{Namespace: "default", Name: "raycluster1"},
		{Namespace: "testns", Name: "raycluster2"},
		{OwnerKind: "rayjob", OwnerName: "rayjob1", Namespace: "default", Name: "raycluster3"},
		{OwnerKind: "rayservice", OwnerName: "rayservice1", Namespace: "testns", Name: "raycluster3"},
	}

	rootDirs := []string{"", "rootdir"}
	for _, rootDir := range rootDirs {
		for _, in := range cases {
			t.Run(rootDir+"/"+in.Namespace+"/"+in.Name, func(t *testing.T) {
				fullKey := EncodePath(in, rootDir, sessionID)

				got, err := DecodePath(fullKey, rootDir)
				if err != nil {
					t.Fatalf("DecodePath(%q) failed: %v", fullKey, err)
				}

				if got.Namespace != in.Namespace || got.Name != in.Name ||
					got.OwnerKind != in.OwnerKind || got.OwnerName != in.OwnerName {
					t.Errorf("Round-trip structure mismatch:\n  in : %+v\n  got: %+v", in, got)
				}
				if got.SessionName != sessionID {
					t.Errorf("SessionName = %q, want %q", got.SessionName, sessionID)
				}
				if got.CreateTimeStamp != sessionTime.Unix() {
					t.Errorf("CreateTimeStamp = %d, want %d", got.CreateTimeStamp, sessionTime.Unix())
				}
			})
		}
	}
}

func TestEncodePath(t *testing.T) {
	tests := []struct {
		name      string
		info      utils.ClusterInfo
		rootDir   string
		sessionID string
		want      string
	}{
		{
			name:      "standalone without owner kind",
			info:      utils.ClusterInfo{Namespace: "ns", Name: "cluster"},
			rootDir:   "myroot",
			sessionID: "session-123",
			want:      "myroot/cluster-metadata/raycluster/ns_cluster/session-123",
		},
		{
			name:      "job with owner kind details",
			info:      utils.ClusterInfo{OwnerKind: "rayjob", OwnerName: "job1", Namespace: "ns", Name: "cluster"},
			rootDir:   "",
			sessionID: "session-456",
			want:      "cluster-metadata/rayjob/ns_job1_cluster/session-456",
		},
		{
			name:      "inconsistent owner details (OwnerKind set but OwnerName empty)",
			info:      utils.ClusterInfo{OwnerKind: "rayjob", OwnerName: "", Namespace: "ns", Name: "cluster"},
			rootDir:   "",
			sessionID: "session-789",
			want:      "cluster-metadata/raycluster/ns_cluster/session-789",
		},
		{
			name:      "job with owner kind details in inconsistent case",
			info:      utils.ClusterInfo{OwnerKind: "RaYjOb", OwnerName: "job1", Namespace: "ns", Name: "cluster"},
			rootDir:   "",
			sessionID: "session-456",
			want:      "cluster-metadata/rayjob/ns_job1_cluster/session-456",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := EncodePath(tc.info, tc.rootDir, tc.sessionID)
			if got != tc.want {
				t.Errorf("EncodePath() = %q, want %q", got, tc.want)
			}
		})
	}
}
