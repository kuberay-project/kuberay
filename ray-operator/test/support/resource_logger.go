package support

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/onsi/gomega/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	PODS_RESOURCE = iota
	JOBS_RESOURCE
	SERVICES_RESOURCE
	RAYJOBS_RESOURCE
	RAYSERVICES_RESOURCE
	RAYCLUSTERS_RESOURCE
)

type ResourceLoggerFunc = func(sb *strings.Builder)

func WithRayJobResourceLogger(t Test) types.GomegaTestingT {
	resources := []int{PODS_RESOURCE, JOBS_RESOURCE, SERVICES_RESOURCE, RAYJOBS_RESOURCE}
	return &RayResourceLogger{t: t, resources: resources}
}

func WithRayServiceResourceLogger(t Test) types.GomegaTestingT {
	resources := []int{PODS_RESOURCE, SERVICES_RESOURCE, RAYSERVICES_RESOURCE, RAYCLUSTERS_RESOURCE}
	return &RayResourceLogger{t: t, resources: resources}
}

func WithRayClusterResourceLogger(t Test) types.GomegaTestingT {
	resources := []int{SERVICES_RESOURCE, RAYCLUSTERS_RESOURCE}
	return &RayResourceLogger{t: t, resources: resources}
}

type RayResourceLogger struct {
	t         Test
	resources []int
}

func (l *RayResourceLogger) Helper() {
	l.t.T().Helper()
}

func (l *RayResourceLogger) Fatalf(format string, args ...any) {
	l.Helper()
	loggers := l.GetLoggers()
	var sb strings.Builder
	// Log the original failure message
	fmt.Fprintf(&sb, format, args...)
	for _, logger := range loggers {
		logger(&sb)
	}
	l.t.T().Fatal(sb.String())
}

func (l *RayResourceLogger) FprintfPods(sb *strings.Builder) {
	if pods, err := l.t.Client().Core().CoreV1().Pods("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== Pods across all namespaces ===\n")
		for _, pod := range pods.Items {
			podJSON, err := json.MarshalIndent(pod, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling pod %s/%s: %v\n", pod.Namespace, pod.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# Pod: %s/%s\n%s\n", pod.Namespace, pod.Name, string(podJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get pods: %v\n", err)
	}
}

func (l *RayResourceLogger) FprintfJobs(sb *strings.Builder) {
	if jobs, err := l.t.Client().Core().BatchV1().Jobs("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== Jobs across all namespaces ===\n")
		for _, job := range jobs.Items {
			jobJSON, err := json.MarshalIndent(job, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling job %s/%s: %v\n", job.Namespace, job.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# Job: %s/%s\n%s\n", job.Namespace, job.Name, string(jobJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get jobs: %v\n", err)
	}
}

func (l *RayResourceLogger) FprintServices(sb *strings.Builder) {
	if services, err := l.t.Client().Core().CoreV1().Services("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== Services across all namespaces ===\n")
		for _, svc := range services.Items {
			serviceJSON, err := json.MarshalIndent(svc, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling service %s/%s: %v\n", svc.Namespace, svc.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# Service: %s/%s\n%s\n", svc.Namespace, svc.Name, string(serviceJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get services: %v\n", err)
	}
}

func (l *RayResourceLogger) FprintRayJobs(sb *strings.Builder) {
	if rayJobs, err := l.t.Client().Ray().RayV1().RayJobs("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== RayJobs across all namespaces ===\n")
		for _, rayJob := range rayJobs.Items {
			rayJobJSON, err := json.MarshalIndent(rayJob, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling rayjob %s/%s: %v\n", rayJob.Namespace, rayJob.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# RayJob: %s/%s\n%s\n", rayJob.Namespace, rayJob.Name, string(rayJobJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get rayjobs: %v\n", err)
	}
}

func (l *RayResourceLogger) FprintRayServices(sb *strings.Builder) {
	if rayServices, err := l.t.Client().Ray().RayV1().RayServices("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== RayServices across all namespaces ===\n")
		for _, rayService := range rayServices.Items {
			rayServiceJSON, err := json.MarshalIndent(rayService, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling rayservice %s/%s: %v\n", rayService.Namespace, rayService.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# RayService: %s/%s\n%s\n", rayService.Namespace, rayService.Name, string(rayServiceJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get rayservices: %v\n", err)
	}
}

func (l *RayResourceLogger) FprintRayClusters(sb *strings.Builder) {
	if rayClusters, err := l.t.Client().Ray().RayV1().RayClusters("").List(l.t.Ctx(), metav1.ListOptions{}); err == nil {
		fmt.Fprintf(sb, "\n=== RayClusters across all namespaces ===\n")
		for _, rayCluster := range rayClusters.Items {
			rayClusterJSON, err := json.MarshalIndent(rayCluster, "", "    ")
			if err != nil {
				fmt.Fprintf(sb, "Error marshaling rayCluster %s/%s: %v\n", rayCluster.Namespace, rayCluster.Name, err)
				continue
			}
			fmt.Fprintf(sb, "---\n# rayCluster: %s/%s\n%s\n", rayCluster.Namespace, rayCluster.Name, string(rayClusterJSON))
		}
	} else {
		fmt.Fprintf(sb, "Failed to get rayCluster: %v\n", err)
	}
}

func (l *RayResourceLogger) MakeFprintUnsupportedResource(resource int) func(sb *strings.Builder) {
	return func(sb *strings.Builder) {
		fmt.Fprintf(sb, "Error: Unsupported resource: %d for RayResourceLogger\n", resource)
	}
}

func (l *RayResourceLogger) GetLoggers() []ResourceLoggerFunc {
	loggers := []ResourceLoggerFunc{}
	for _, resource := range l.resources {
		switch resource {
		case PODS_RESOURCE:
			loggers = append(loggers, l.FprintfPods)
		case JOBS_RESOURCE:
			loggers = append(loggers, l.FprintfJobs)
		case SERVICES_RESOURCE:
			loggers = append(loggers, l.FprintServices)
		case RAYJOBS_RESOURCE:
			loggers = append(loggers, l.FprintRayJobs)
		case RAYSERVICES_RESOURCE:
			loggers = append(loggers, l.FprintRayServices)
		case RAYCLUSTERS_RESOURCE:
			loggers = append(loggers, l.FprintRayClusters)
		default:
			loggers = append(loggers, l.MakeFprintUnsupportedResource(resource))
		}
	}
	return loggers
}
