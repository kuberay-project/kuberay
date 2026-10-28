package config

import (
	"k8s.io/apimachinery/pkg/util/sets"
)

type EnterConfig struct {
	Module             string
	ModuleExecBinary   string
	WatchLogDir        string
	EnterOssExpireHour int
	EnableMeta         bool
}

type GlobalConfig struct {
	OSSEndpoint         string
	OSSRegion           string
	OSSBucket           string
	OSSHistoryServerDir string
}

type RayMetaHanderConfig struct {
	GlobalConfig
	RayClusterName      string
	RayClusterNamespace string
	OSSExpireHour       int
}

type RayHistoryServerConfig struct {
	AllowedUID sets.Set[string]
	AllowedAID sets.Set[string]
	GlobalConfig
	DashBoardDir     string
	ArmsRegionId     string
	ACKClusterId     string
	WebAppId         string
	WebAppSecret     string
	LocalServiceName string
	MaxOssListLimit  int
}
