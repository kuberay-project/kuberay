package config

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateGlobalConfig is
func ValidateGlobalConfig(c *GlobalConfig, fldpath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(c.OSSEndpoint) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.OSSEndpoint, "oss_endpoint must be set"))
	}
	if len(c.OSSRegion) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.OSSRegion, "oss_region must be set"))
	}
	if len(c.OSSBucket) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.OSSBucket, "oss_bucket must be set"))
	}
	if len(c.OSSHistoryServerDir) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.OSSHistoryServerDir, "oss_historyserver_root_dir must be set"))
	}

	return allErrs
}

// ValidateMetaHanderConfig is
func ValidateMetaHanderConfig(c *RayMetaHanderConfig, fldpath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(c.RayClusterName) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterName, "ray_cluster_name must be set"))
	}
	if len(c.RayClusterNamespace) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterNamespace, "ray_cluster_namespace must be set"))
	}
	return allErrs
}

func ValidatRayHistoryServerConfig(c *RayHistoryServerConfig, fldpath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(c.DashBoardDir) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.DashBoardDir, "dashboard-dir must be set"))
	}
	if len(c.LocalServiceName) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.LocalServiceName, "local_service_name must be set"))
	}
	if len(c.WebAppId) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.WebAppId, "webapp_id must be set"))
	}
	if len(c.WebAppSecret) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.WebAppSecret, "webapp_secret must be set"))
	}

	return allErrs
}
