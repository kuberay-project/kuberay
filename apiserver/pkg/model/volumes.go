package model

import (
	corev1 "k8s.io/api/core/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
)

func PopulateVolumes(podTemplate *corev1.PodTemplateSpec) []*api.Volume {
	if len(podTemplate.Spec.Volumes) == 0 {
		return nil
	}
	var volumes []*api.Volume
	for _, vol := range podTemplate.Spec.Volumes {
		mount := GetVolumeMount(podTemplate, vol.Name)
		if vol.VolumeSource.ConfigMap != nil {
			volume := api.Volume{
				Name:       vol.Name,
				MountPath:  mount.MountPath,
				VolumeType: api.Volume_CONFIGMAP,
				Source:     vol.ConfigMap.LocalObjectReference.Name,
			}
			if len(vol.ConfigMap.Items) > 0 {
				items := make(map[string]string)
				for _, element := range vol.ConfigMap.Items {
					items[element.Key] = element.Path
				}
				volume.Items = items
			}
			volumes = append(volumes, &volume)
			continue
		}
		if vol.VolumeSource.Secret != nil {
			volume := api.Volume{
				Name:       vol.Name,
				MountPath:  mount.MountPath,
				VolumeType: api.Volume_SECRET,
				Source:     vol.Secret.SecretName,
			}
			if len(vol.Secret.Items) > 0 {
				items := make(map[string]string)
				for _, element := range vol.ConfigMap.Items {
					items[element.Key] = element.Path
				}
				volume.Items = items
			}
			volumes = append(volumes, &volume)
			continue
		}
		if vol.VolumeSource.EmptyDir != nil {
			volume := api.Volume{
				Name:       vol.Name,
				MountPath:  mount.MountPath,
				VolumeType: api.Volume_EMPTY_DIR,
			}
			if vol.EmptyDir.SizeLimit != nil {
				volume.Storage = vol.EmptyDir.SizeLimit.String()
			}
			volumes = append(volumes, &volume)
			continue
		}
		if vol.VolumeSource.HostPath != nil {
			// Host Path
			volumes = append(volumes, &api.Volume{
				Name:                 vol.Name,
				MountPath:            mount.MountPath,
				Source:               vol.VolumeSource.HostPath.Path,
				MountPropagationMode: GetVolumeMountPropagation(mount),
				VolumeType:           api.Volume_VolumeType(api.Volume_HOSTTOCONTAINER),
				HostPathType:         GetVolumeHostPathType(&vol),
			})
			continue

		}
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			// PVC
			volumes = append(volumes, &api.Volume{
				Name:                 vol.Name,
				MountPath:            mount.MountPath,
				MountPropagationMode: GetVolumeMountPropagation(mount),
				VolumeType:           api.Volume_PERSISTENT_VOLUME_CLAIM,
				Source:               vol.VolumeSource.PersistentVolumeClaim.ClaimName,
				ReadOnly:             vol.VolumeSource.PersistentVolumeClaim.ReadOnly,
			})
			continue
		}
		if vol.VolumeSource.Ephemeral != nil {
			// Ephimeral
			request := vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
			volume := api.Volume{
				Name:                 vol.Name,
				MountPath:            mount.MountPath,
				MountPropagationMode: GetVolumeMountPropagation(mount),
				VolumeType:           api.Volume_EPHEMERAL,
				AccessMode:           GetAccessMode(&vol),
				Storage:              request.String(),
			}
			if vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName != nil {
				volume.StorageClassName = *vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName
			}
			volumes = append(volumes, &volume)
			continue
		}
	}
	return volumes
}

func GetVolumeMount(podTemplate *corev1.PodTemplateSpec, vol string) *corev1.VolumeMount {
	for _, container := range podTemplate.Spec.Containers {
		for _, mount := range container.VolumeMounts {
			if mount.Name == vol {
				return &mount
			}
		}
	}
	return nil
}

func GetVolumeMountPropagation(mount *corev1.VolumeMount) api.Volume_MountPropagationMode {
	if mount.MountPropagation == nil {
		return api.Volume_NONE
	}
	if *mount.MountPropagation == corev1.MountPropagationHostToContainer {
		return api.Volume_HOSTTOCONTAINER
	}
	if *mount.MountPropagation == corev1.MountPropagationBidirectional {
		return api.Volume_BIDIRECTIONAL
	}
	return api.Volume_NONE
}

func GetVolumeHostPathType(vol *corev1.Volume) api.Volume_HostPathType {
	if vol.VolumeSource.HostPath.Type == nil {
		return api.Volume_DIRECTORY
	}
	if *vol.VolumeSource.HostPath.Type == corev1.HostPathFile {
		return api.Volume_FILE
	}
	return api.Volume_DIRECTORY
}

func GetAccessMode(vol *corev1.Volume) api.Volume_AccessMode {
	modes := vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.AccessModes
	if len(modes) == 0 {
		return api.Volume_RWO
	}
	if modes[0] == corev1.ReadOnlyMany {
		return api.Volume_ROX
	}
	if modes[0] == corev1.ReadWriteMany {
		return api.Volume_RWX
	}
	return api.Volume_RWO
}
