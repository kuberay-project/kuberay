package util

const releaseTestDir = "release-test" // under the repository root

// A results directory holds SUMMARY.md and a directory per unit with its report.md and steps/ logs.
// The skills and summary.md.tmpl refer to these names too.
const (
	SummaryFile = "SUMMARY.md"
	ReportFile  = "report.md"
	StepsDir    = "steps"
)

// ReleaseVersion is a release's version: its tag without the v, and its chart version.
const ReleaseVersion = `[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[0-9]+)?`

// KuberayVersionRefs are where a command names a KubeRay version, each followed by the version.
// validation.go flags a fixed or floating one there; source/source.go ignores a release version there.
const KuberayVersionRefs = `ray-project/kuberay/(?:blob/|tree/|releases/download/)?(?:refs/(?:heads|tags)/)?` + // GitHub
	`|ray-project/kuberay\S*(?:\?ref=|@)` + // kustomize and go
	`|quay\.io/kuberay/[a-z-]+:` + // images
	`|kuberay/(?:kuberay-operator|ray-cluster|kuberay-apiserver)[^\n|;&]*--version[= ]+` // Helm charts

const (
	DocsSite   = "https://docs.ray.io/"
	DocsMaster = DocsSite + "en/master/"
	// RayDocsRaw is a docs page's source on ray master: path under doc/source/, extension.
	RayDocsRaw = "https://raw.githubusercontent.com/ray-project/ray/master/doc/source/%s.%s"
)

// Steps see the tag under test and its chart version, the tag without the v, in these.
const (
	EnvKuberayVersion = "KUBERAY_VERSION"
	EnvChartVersion   = "CHART_VERSION"
)

const DefaultTimeout = 300 // seconds, for a Step without a timeout and for each cleanup command

// A report's status: pass and fail come from running the unit, stale from its source changing.
const (
	StatusPass  = "pass"
	StatusFail  = "fail"
	StatusStale = "stale"
)
