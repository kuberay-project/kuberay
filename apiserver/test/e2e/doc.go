// Package e2e provides test functions, utility function and structs that allow for integration testing
// of Kuberay API server and Kuberay operator.
//
// The code assumes that cluster found in [~/.kube/config] up and has the needed components (Kuberay API server
// Kuberay Operator) deployed and functional.
//
// The code is organized as follows:
//
//   - types.go -- provides for data types
//   - utils.go -- provides for utility functions
//   - exec.go -- provides functions for exec command with kubectl exec
//   - cluster_server_e2e_test.go -- provides the test function for the Cluster GRPC Server
//   - config_server_e2e_test.go -- provides the test function for the Config GRPC Server
//   - job_server_e2e_test.go -- provides the test function for the Job GRPC Server
//   - service_server_e2e_test.go -- provides the test function
package e2e
