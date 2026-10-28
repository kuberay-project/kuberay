package model

import (
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
)

var (
	hostToContainer = corev1.MountPropagationHostToContainer
	bidirectonal    = corev1.MountPropagationBidirectional
	sizelimit       = resource.MustParse("100Gi")
)

var podTemplateTest = corev1.PodTemplateSpec{
	Spec: corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "ray-head",
				Image: "blah",
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:             "hostPath",
						MountPath:        "/tmp/hostPath",
						MountPropagation: &hostToContainer,
					},
					{
						Name:             "pvc",
						MountPath:        "/tmp/pvc",
						MountPropagation: &bidirectonal,
					},
					{
						Name:      "ephemeral",
						MountPath: "/tmp/ephemeral",
					},
					{
						Name:      "configMap",
						MountPath: "/tmp/configmap",
					},
					{
						Name:      "secret",
						MountPath: "/tmp/secret",
					},
					{
						Name:      "emptyDir",
						MountPath: "/tmp/emptydir",
					},
				},
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "hostPath",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/tmp",
						Type: newHostPathType(string(corev1.HostPathDirectory)),
					},
				},
			},
			{
				Name: "pvc",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "pvcclaim",
						ReadOnly:  false,
					},
				},
			},
			{
				Name: "ephemeral",
				VolumeSource: corev1.VolumeSource{
					Ephemeral: &corev1.EphemeralVolumeSource{
						VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									"app.kubernetes.io/managed-by": "kuberay-apiserver",
								},
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse("5Gi"),
									},
								},
							},
						},
					},
				},
			},
			{
				Name: "configMap",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "my-config-map",
						},
						Items: []corev1.KeyToPath{
							{
								Key:  "key1",
								Path: "path1",
							},
							{
								Key:  "key2",
								Path: "path2",
							},
						},
					},
				},
			},
			{
				Name: "secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: "my-secret",
					},
				},
			},
			{
				Name: "emptyDir",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						SizeLimit: &sizelimit,
					},
				},
			},
		},
	},
}

var expectedVolumes = []*api.Volume{
	{
		Name:                 "hostPath",
		Source:               "/tmp",
		MountPath:            "/tmp/hostPath",
		VolumeType:           api.Volume_HOST_PATH,
		HostPathType:         api.Volume_DIRECTORY,
		MountPropagationMode: api.Volume_HOSTTOCONTAINER,
	},
	{
		Name:                 "pvc",
		MountPath:            "/tmp/pvc",
		Source:               "pvcclaim",
		VolumeType:           api.Volume_PERSISTENT_VOLUME_CLAIM,
		MountPropagationMode: api.Volume_BIDIRECTIONAL,
		ReadOnly:             false,
	},
	{
		Name:                 "ephemeral",
		MountPath:            "/tmp/ephemeral",
		MountPropagationMode: api.Volume_NONE,
		VolumeType:           api.Volume_EPHEMERAL,
		Storage:              "5Gi",
		AccessMode:           api.Volume_RWO,
	},
	{
		Name:       "configMap",
		MountPath:  "/tmp/configmap",
		VolumeType: api.Volume_CONFIGMAP,
		Source:     "my-config-map",
		Items: map[string]string{
			"key1": "path1",
			"key2": "path2",
		},
	},
	{
		Name:       "secret",
		MountPath:  "/tmp/secret",
		VolumeType: api.Volume_SECRET,
		Source:     "my-secret",
	},
	{
		Name:       "emptyDir",
		MountPath:  "/tmp/emptydir",
		VolumeType: api.Volume_EMPTY_DIR,
		Storage:    "100Gi",
	},
}

// Build host path
func newHostPathType(pathType string) *corev1.HostPathType {
	hostPathType := new(corev1.HostPathType)
	*hostPathType = corev1.HostPathType(pathType)
	return hostPathType
}

func TestPopulateVolumes(t *testing.T) {
	volumes := PopulateVolumes(&podTemplateTest)
	for i, vol := range volumes {
		fmt.Printf("volume = %#v\n", vol)
		if !reflect.DeepEqual(vol, expectedVolumes[i]) {
			t.Errorf("failed volumes conversion, got %v, expected %v", volumes, expectedVolumes)
		}
	}
}

func TestGetVolumeHostPathType(t *testing.T) {
	vol1 := corev1.Volume{
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/tmp",
				Type: nil,
			},
		},
	}
	result1 := GetVolumeHostPathType(&vol1)
	if result1 != api.Volume_DIRECTORY {
		t.Errorf("Expected FILE type, got %v", result1)
	}

	typeFile := corev1.HostPathFile
	vol2 := corev1.Volume{
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/tmp",
				Type: &typeFile,
			},
		},
	}
	result2 := GetVolumeHostPathType(&vol2)
	if result2 != api.Volume_FILE {
		t.Errorf("Expected FILE type, got %v", result2)
	}

	typeDir := corev1.HostPathDirectory
	vol3 := corev1.Volume{
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/tmp",
				Type: &typeDir,
			},
		},
	}
	result3 := GetVolumeHostPathType(&vol3)
	if result3 != api.Volume_DIRECTORY {
		t.Errorf("Expected DIRECTORY type, got %v", result3)
	}
}
