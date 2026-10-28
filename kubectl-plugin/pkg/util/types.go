package util

type ResourceType string

const (
	RayCluster ResourceType = "raycluster"
	RayJob     ResourceType = "rayjob"
	RayService ResourceType = "rayservice"
)
