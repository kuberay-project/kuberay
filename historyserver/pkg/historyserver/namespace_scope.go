package historyserver

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

var errNamespaceNotAllowed = errors.New("namespace is outside the configured History Server scope")

// namespaceScope limits the namespaces served by one History Server instance.
// Its zero value preserves access to every namespace permitted by the credentials.
type namespaceScope struct {
	namespace string
	prefix    string
}

func newNamespaceScope(namespace, prefix string) (namespaceScope, error) {
	if namespace != "" && prefix != "" {
		return namespaceScope{}, fmt.Errorf("--namespace and --namespace-prefix are mutually exclusive")
	}
	if namespace != "" && len(validation.IsDNS1123Label(namespace)) != 0 {
		return namespaceScope{}, fmt.Errorf("invalid namespace %q", namespace)
	}
	// A prefix may end in hyphens even though a complete namespace cannot.
	if prefix != "" && (len(prefix) > 63 || len(validation.IsDNS1123Label(strings.TrimRight(prefix, "-"))) != 0) {
		return namespaceScope{}, fmt.Errorf("invalid namespace prefix %q: use a literal Kubernetes namespace prefix", prefix)
	}
	return namespaceScope{namespace: namespace, prefix: prefix}, nil
}

func (s namespaceScope) allows(namespace string) bool {
	if s.namespace == "" && s.prefix == "" {
		return true
	}
	if len(validation.IsDNS1123Label(namespace)) != 0 {
		return false
	}
	if s.namespace != "" {
		return namespace == s.namespace
	}
	return strings.HasPrefix(namespace, s.prefix)
}

func (s *ServerHandler) namespaceAllowed(namespace string) bool {
	return s.clientManager == nil || s.clientManager.namespaceScope.allows(namespace)
}
