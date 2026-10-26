package historyserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"mime"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/emicklei/go-restful/v3"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/html"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

const (
	COOKIE_CLUSTER_NAME_KEY      = "cluster_name"
	COOKIE_CLUSTER_NAMESPACE_KEY = "cluster_namespace"
	COOKIE_SESSION_NAME_KEY      = "session_name"
	COOKIE_OWNER_KIND_KEY        = "owner_kind"
	COOKIE_OWNER_NAME_KEY        = "owner_name"
	COOKIE_DASHBOARD_VERSION_KEY = "dashboard_version"

	ATTRIBUTE_SERVICE_NAME = "cluster_service_name"
	ATTRIBUTE_AUTH_TOKEN   = "cluster_auth_token"
)

// handleMissingSnapshot responds 503 when the session snapshot is not in the cache.
func (s *ServerHandler) handleMissingSnapshot(resp *restful.Response) {
	resp.WriteErrorString(http.StatusServiceUnavailable,
		"session snapshot not in cache; reload via /enter_cluster")
}

type ServiceInfo struct {
	ServiceName string
	Namespace   string
	Port        int
}

func RequestLogFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	logrus.Infof("Received request: %s %s", req.Request.Method, req.Request.URL.String())
	chain.ProcessFilter(req, resp)
}

func routerClusters(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)

	ws.Path("/clusters").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/").To(s.getClusters).
		Doc("get all clusters").
		Writes([]string{}))
}

func routerTimezone(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/timezone").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Route(ws.GET("/").To(s.getTimezone).Filter(s.CookieHandle).
		Doc("get timezone").
		Writes(""))
}

// routerNodes registers RESTful routers for node-related endpoints.
// It sets up two routes:
//   - GET /nodes: retrieves all node information for a given cluster
//   - GET /nodes/{node_id}: retrieves node details for a specific node by its ID
//
// Supported view parameters for GET /nodes:
//   - ?view=summary: returns node summary and resource usage information (default)
//   - ?view=hostNameList: returns a list of hostnames for all alive nodes
func routerNodes(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)

	ws.Path("/nodes").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON) //.Filter(s.LoginWrapper)

	ws.Route(ws.GET("").To(s.getNodes).
		Filter(s.CookieHandle).
		Doc("Get all node information for a given cluster").
		Param(ws.QueryParameter("view", "View type: 'summary' (default) for node summary and resources, 'hostNameList' for alive node hostnames")).
		Writes(""))

	ws.Route(ws.GET("/{node_id}").To(s.getNode).
		Filter(s.CookieHandle).
		Doc("Get node summary for a specific node by its ID").
		Param(ws.PathParameter("node_id", "The unique identifier of the node")).
		Writes(""))
}

func routerEvents(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/events").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/").To(s.getEvents).Filter(s.CookieHandle).
		Doc("get events").
		Writes(""))
}

func routerAPI(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/api").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/cluster_status").To(s.getClusterStatus).Filter(s.CookieHandle).
		Doc("get clusters status").Param(ws.QueryParameter("format", "such as 1")).
		Writes("")) // Placeholder for specific return type
	ws.Route(ws.GET("/grafana_health").To(s.getGrafanaHealth).Filter(s.CookieHandle).
		Doc("get grafana_health").
		Writes("")) // Placeholder for specific return type
	ws.Route(ws.GET("/prometheus_health").To(s.getPrometheusHealth).Filter(s.CookieHandle).
		Doc("get prometheus_health").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/jobs/").To(s.getJobs).Filter(s.CookieHandle).
		Doc("get driver jobs").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/jobs/{job_id}").To(s.getJob).Filter(s.CookieHandle).
		Doc("get single driver job").
		Param(ws.PathParameter("job_id", "job_id")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/cluster_metadata").To(s.getClusterMetadata).Filter(s.CookieHandle).
		Doc("get cluster metadata (Ray version, Python version, etc.)").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/logs").To(s.getNodeLogs).Filter(s.CookieHandle).
		Doc("get logs").
		Param(ws.QueryParameter("node_id", "node_id")).
		Param(ws.QueryParameter("glob", "glob pattern")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/logs/{media_type}").To(s.getNodeLog).Filter(s.CookieHandle).
		Doc("get or stream logs").
		Param(ws.PathParameter("media_type", "media type: 'file' for log file content, 'stream' for real-time streaming")).
		Param(ws.QueryParameter("node_id", "node_id (optional if node_ip/task_id/actor_id is provided)")).
		Param(ws.QueryParameter("node_ip", "node_ip (optional, resolve to node_id)")).
		Param(ws.QueryParameter("filename", "filename (explicit log file path)")).
		Param(ws.QueryParameter("task_id", "task_id (resolve log file from task)")).
		Param(ws.QueryParameter("actor_id", "actor_id (resolve log file from actor)")).
		Param(ws.QueryParameter("pid", "pid (resolve log file from process id)")).
		Param(ws.QueryParameter("suffix", "suffix (out or err, default: out, used with task_id/actor_id/pid)")).
		Param(ws.QueryParameter("lines", "lines (number of lines to return, default: 1000, used with media_type=file)")).
		Param(ws.QueryParameter("timeout", "timeout")).
		Param(ws.QueryParameter("attempt_number", "attempt_number (task retry attempt number, default: 0, used with media_type=file)")).
		Param(ws.QueryParameter("download_filename", "download_filename (if set, triggers download with this filename, used with media_type=file)")).
		Param(ws.QueryParameter("filter_ansi_code", "filter_ansi_code (true/false, used with media_type=file)")).
		Param(ws.QueryParameter("interval", "interval (polling interval in seconds, used with media_type=stream)")).
		// Note: submission_id is not supported and not needed.
		// The Ray Dashboard frontend does not use submission_id as a query param.
		// Instead, it embeds the submission_id directly into the filename param
		// (e.g. filename=job-driver-raysubmit_xxx.log), which is already supported.
		Produces("text/plain", "text/event-stream").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/tasks").To(s.getTasks).Filter(s.CookieHandle).
		Doc("get task detail").
		Param(ws.QueryParameter("limit", "limit")).
		Param(ws.QueryParameter("timeout", "timeout")).
		Param(ws.QueryParameter("detail", "detail")).
		Param(ws.QueryParameter("exclude_driver", "exclude_driver")).
		Param(ws.QueryParameter("filter_keys", "filter_keys")).
		Param(ws.QueryParameter("filter_predicates", "filter_predicates")).
		Param(ws.QueryParameter("filter_values", "filter_values")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/tasks/summarize").To(s.getTaskSummarize).Filter(s.CookieHandle).
		Doc("get summarize").
		Param(ws.QueryParameter("filter_keys", "filter_keys")).
		Param(ws.QueryParameter("filter_predicates", "filter_predicates")).
		Param(ws.QueryParameter("filter_values", "filter_values")).
		Param(ws.QueryParameter("summary_by", "summary_by")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/tasks/timeline").To(s.getTasksTimeline).Filter(s.CookieHandle).
		Doc("get tasks timeline").
		Param(ws.QueryParameter("job_id", "filter by job_id")).
		Param(ws.QueryParameter("download", "set to 1 to return response as attachment (timeline JSON file)")).
		Produces(restful.MIME_JSON).
		Writes("")) // Placeholder for specific return type

	// Fallback route for built-in and user-configured endpoints fetched by the collector.
	// This must be registered last because go-restful matches more specific routes first.
	ws.Route(ws.GET("/{subpath:*}").To(s.getFetchedEndpoint).Filter(s.CookieHandle).
		Doc("fallback handler for endpoints fetched and stored by the collector").
		Writes(""))
}

// func routerRoot(s *ServerHandler) {
// 	ws := new(restful.WebService)
// 	defer restful.Add(ws)
// 	ws.Filter(RequestLogFilter)
// 	ws.Route(ws.GET("/").To(func(req *restful.Request, w *restful.Response) {
// 		isHomePage := true
// 		_, err := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
// 		isHomePage = err != nil
// 		prefix := ""
// 		if isHomePage {
// 			prefix = "homepage"
// 		} else {
// 			version := "v2.51.0"
// 			if versionCookie, err := req.Request.Cookie(COOKIE_DASHBOARD_VERSION_KEY); err == nil {
// 				version = versionCookie.Value
// 			}
// 			prefix = version + "/client/build"
// 		}
// 		// Check if homepage file exists; if so use it, otherwise use default index.html
// 		homepagePath := path.Join(s.dashboardDir, prefix, "index.html")

// 		var data []byte

// 		if _, statErr := os.Stat(homepagePath); !os.IsNotExist(statErr) {
// 			data, err = os.ReadFile(homepagePath)
// 		} else {
// 			http.Error(w, "could not read HTML file", http.StatusInternalServerError)
// 			logrus.Errorf("could not read HTML file: %v", statErr)
// 			return
// 		}

// 		if err != nil {
// 			http.Error(w, "could not read HTML file", http.StatusInternalServerError)
// 			logrus.Errorf("could not read HTML file: %v", err)
// 			return
// 		}
// 		w.Header().Set("Content-Type", "text/html")
// 		w.Write(data)
// 	}).Writes(""))
// }

// TODO: this is the frontend's entry.
// func routerHomepage(s *ServerHandler) {
// 	ws := new(restful.WebService)
// 	defer restful.Add(ws)
// 	ws.Path("/homepage").Consumes("*/*").Produces("*/*").Filter(RequestLogFilter)
// 	ws.Route(ws.GET("/").To(func(_ *restful.Request, w *restful.Response) {
// 		data, err := os.ReadFile(path.Join(s.dashboardDir, "homepage/index.html"))
// 		if err != nil {
// 			// Fallback to root path
// 			routerRoot(s)
// 			return
// 		}
// 		w.Header().Set("Content-Type", "text/html")
// 		w.Write(data)
// 	}).Writes(""))
// }

func routerHealthz(_ *ServerHandler) {
	http.HandleFunc("/readz", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Received request: %s %s", r.Method, r.URL.String())
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
		logrus.Debugf("request /readz")
	})
	http.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Received request: %s %s", r.Method, r.URL.String())
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
		logrus.Debugf("request /livez")
	})
}

func routerSelectCluster(_ *ServerHandler) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Serving cluster selector page: %s %s", r.Method, r.URL.String())
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'")
		w.Write(html.ClusterSelectorHTML)
	}
	http.HandleFunc("/select_cluster", handler)
}

func routerLogical(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/logical").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter) //.Filter(s.loginWrapper)
	// Note: The Ray Dashboard frontend calls GET /logical/actors without any filter params
	// and does client-side filtering, so filter parameters are not needed for this endpoint.
	ws.Route(ws.GET("/actors").To(s.getLogicalActors).Filter(s.CookieHandle).
		Doc("get logical actors").
		Writes("")) // Placeholder for specific return type

	// TODO: discuss with Ray Core team about this
	// I noticed that IDs (`actor_id`, `job_id`, `node_id`, etc.) in Ray Base Events
	// are encoded as Base64, while the Dashboard/State APIs use Hex.
	// Problem: Base64 can contain `/` characters, which breaks URL routing:
	ws.Route(ws.GET("/actors/{single_actor:*}").To(s.getLogicalActor).Filter(s.CookieHandle).
		Doc("get logical single actor").
		Param(ws.PathParameter("single_actor", "single_actor")).
		Writes("")) // Placeholder for specific return type
}

func routerRayClusterSet(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)

	ws.Path("/enter_cluster").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter)
	enterHandler := func(r1 *restful.Request, r2 *restful.Response, namespace, resourceType, resourceName, session string) {
		kindLower := strings.ToLower(resourceType)
		if kindLower != utils.RayClusterKind && kindLower != utils.RayJobKind && kindLower != utils.RayServiceKind {
			r2.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("unsupported resource kind: %q (must be raycluster, rayjob, or rayservice)", resourceType))
			return
		}

		resolvedClusterInfo, found, err := s.resolveSession(r1.Request.Context(), namespace, resourceType, resourceName, session)
		if err != nil {
			logrus.Errorf("Failed to resolve session %s/%s/%s/%s: %v", namespace, resourceType, resourceName, session, err)
			r2.WriteErrorString(http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			r2.WriteErrorString(http.StatusNotFound, fmt.Sprintf("cluster %s/%s/%s with session %s not found", namespace, resourceType, resourceName, session))
			return
		}

		resolvedName := resolvedClusterInfo.Name
		resolvedSession := resolvedClusterInfo.SessionName

		if resolvedSession != "live" {
			if ParseSessionTimestamp(resolvedSession).IsZero() {
				logrus.Warnf("Rejecting invalid session name: %s/%s/%s", namespace, resolvedName, resolvedSession)
				r2.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("invalid session name: %q", resolvedSession))
				return
			}
			live, err := s.sessionLoader.LoadSession(r1.Request.Context(), resolvedClusterInfo)
			if err != nil {
				logrus.Errorf("Failed to load session %s/%s/%s: %v", namespace, resolvedName, resolvedSession, err)
				r2.WriteErrorString(http.StatusInternalServerError, err.Error())
				return
			}

			// Users might use the complete session name to enter a live cluster,
			// so "live" sentinel is set to avoid querying empty in-memory state.
			if live {
				if !s.enableLiveClusters {
					r2.WriteErrorString(http.StatusNotFound, fmt.Sprintf(
						"RayCluster %s/%s is still running, so session %q has no stored replay to serve; --enable-live-clusters is disabled on this History Server",
						namespace, resolvedName, resolvedSession))
					return
				}
				resolvedSession = "live"
			}
		}

		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAME_KEY, Value: resolvedName})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAMESPACE_KEY, Value: namespace})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_SESSION_NAME_KEY, Value: resolvedSession})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_OWNER_KIND_KEY, Value: resolvedClusterInfo.OwnerKind})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_OWNER_NAME_KEY, Value: resolvedClusterInfo.OwnerName})

		r2.WriteJson(map[string]any{
			"result":    "success",
			"name":      resolvedName,
			"namespace": namespace,
			"session":   resolvedSession,
		}, "application/json")
	}

	ws.Route(ws.GET("/{namespace}/{kind}/{name}").To(func(r1 *restful.Request, r2 *restful.Response) {
		namespace := r1.PathParameter("namespace")
		kind := r1.PathParameter("kind")
		name := r1.PathParameter("name")
		enterHandler(r1, r2, namespace, kind, name, "latest")
	}).
		Doc("set cookie for cluster (defaults session to latest)").
		Param(ws.PathParameter("namespace", "namespace")).
		Param(ws.PathParameter("kind", "kind (raycluster, rayjob, or rayservice)")).
		Param(ws.PathParameter("name", "name")).
		Writes(""))

	ws.Route(ws.GET("/{namespace}/{kind}/{name}/{session}").To(func(r1 *restful.Request, r2 *restful.Response) {
		namespace := r1.PathParameter("namespace")
		kind := r1.PathParameter("kind")
		name := r1.PathParameter("name")
		session := r1.PathParameter("session")
		enterHandler(r1, r2, namespace, kind, name, session)
	}).
		Doc("set cookie for cluster").
		Param(ws.PathParameter("namespace", "namespace")).
		Param(ws.PathParameter("kind", "kind (raycluster, rayjob, or rayservice)")).
		Param(ws.PathParameter("name", "name")).
		Param(ws.PathParameter("session", "session")).
		Writes(""))
}

func (s *ServerHandler) RegisterRouter() {
	routerRayClusterSet(s)
	routerClusters(s)
	routerTimezone(s)
	routerNodes(s)
	routerEvents(s)
	routerAPI(s)
	routerSelectCluster(s)
	// routerRoot(s)
	// routerHomepage(s)
	routerHealthz(s)
	routerLogical(s)
}

func (s *ServerHandler) redirectRequest(req *restful.Request, resp *restful.Response) {
	if !s.enableLiveClusters {
		resp.WriteErrorString(http.StatusForbidden, "live cluster access is disabled on this History Server")
		return
	}

	svcInfo, ok := req.Attribute(ATTRIBUTE_SERVICE_NAME).(ServiceInfo)
	if !ok {
		logrus.Errorf("Cannot proxy %s: head service info missing from the request context", req.Request.URL.String())
		resp.WriteErrorString(http.StatusInternalServerError, "head service info missing from the request context")
		return
	}

	var targetURL string
	if s.useKubernetesProxy {
		// Use Kubernetes API server proxy to access the in-cluster RayDashboard services.
		targetURL = fmt.Sprintf("%s/api/v1/namespaces/%s/services/%s:dashboard/proxy%s",
			s.clientManager.configs[0].Host,
			svcInfo.Namespace,
			svcInfo.ServiceName,
			req.Request.URL.String())
		logrus.Infof("Using Kubernetes API server proxy to access service %s/%s: %s",
			svcInfo.Namespace, svcInfo.ServiceName, req.Request.URL.String())
	} else {
		// Connect through in-cluster service discovery.
		targetURL = fmt.Sprintf("http://%s:%d%s", svcInfo.ServiceName, svcInfo.Port, req.Request.URL.String())
		logrus.Infof("Using in-cluster service discovery to access service %s/%s: %s",
			svcInfo.Namespace, svcInfo.ServiceName, req.Request.URL.String())
	}

	// Create a new request to the target URL.
	proxyReq, err := http.NewRequestWithContext(req.Request.Context(), req.Request.Method, targetURL, req.Request.Body)
	if err != nil {
		logrus.Errorf("Failed to create proxy request: %v", err)
		resp.WriteError(http.StatusInternalServerError, err)
		return
	}

	// Copy headers from original request to proxy request.
	for key, values := range req.Request.Header {
		if strings.EqualFold(key, "host") {
			continue
		}
		// In auth-token mode, drop the client-supplied x-ray-authorization so it cannot bypass
		// the server-managed token injected below.
		if s.useAuthTokenMode && strings.EqualFold(key, utils.RayAuthHeader) {
			continue
		}
		// Authorization has to be dropped in two cases. In auth-token mode, Ray only falls back to
		// x-ray-authorization when Authorization is absent, so a forwarded value would win over the
		// injected token. On the Kubernetes proxy path the header is addressed to the API server
		// instead of Ray, and client-go's transport skips injecting the service account token when
		// Authorization is already set, so forwarding it would make the proxied request fail
		// authentication.
		if (s.useAuthTokenMode || s.useKubernetesProxy) && strings.EqualFold(key, "authorization") {
			continue
		}
		for _, value := range values {
			// Use Add() to preserve multiple values for the same header key.
			proxyReq.Header.Add(key, value)
		}
	}

	// Add auth token header if auth token mode is enabled
	if s.useAuthTokenMode {
		// Get the auth token from request attribute (set by CookieHandle filter). An empty token means
		// auth is disabled on the target cluster (the only case GetAuthTokenForRayCluster returns one),
		// so no header is needed; auth-enabled clusters without a token fail before reaching here.
		if authTokenAttr := req.Attribute(ATTRIBUTE_AUTH_TOKEN); authTokenAttr != nil {
			if authToken, ok := authTokenAttr.(string); ok && authToken != "" {
				proxyReq.Header.Set(utils.RayAuthHeader, fmt.Sprintf("Bearer %s", authToken))
			}
		}
	}

	// Send the proxy request to the target URL.
	remoteResp, err := s.httpClient.Do(proxyReq)
	if err != nil {
		logrus.Errorf("Failed to proxy request to %s: %v", targetURL, err)
		resp.WriteError(http.StatusBadGateway, err)
		return
	}
	defer remoteResp.Body.Close()

	// Copy headers from remote response.
	for key, values := range remoteResp.Header {
		for _, value := range values {
			resp.Header().Add(key, value)
		}
	}

	// Set status code.
	resp.WriteHeader(remoteResp.StatusCode)

	// Copy response body.
	_, err = io.Copy(resp, remoteResp.Body)
	if err != nil {
		logrus.Errorf("Failed to copy response body: %v", err)
	}
}

func (s *ServerHandler) getClusters(_ *restful.Request, resp *restful.Response) {
	clusters := s.listClusters(s.maxClusters)
	resp.WriteAsJson(clusters)
}

// getNodes retrieves all node summaries and resource usage information for a specific cluster session.
// The API schema of live and dead clusters are different:
//   - Live clusters: returns the current snapshot
//   - Dead clusters: returns the historical replay
//
// Supported view parameters:
//   - ?view=summary: returns node summary and resource usage information
//   - ?view=hostNameList: returns a list of hostnames for all alive nodes
func (s *ServerHandler) getNodes(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Parse query parameters.
	viewParam := req.QueryParameter("view")

	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	// Handle different view types.
	switch viewParam {
	case "hostNameList":
		s.getNodesHostNameList(snap.Nodes, resp)
	case "summary", "":
		// Default to summary view
		s.getNodesSummary(snap.Nodes, sessionName, resp)
	default:
		resp.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("unsupported view parameter: %s", viewParam))
	}
}

// getNodesSummary returns node summary and resource usage information for historical clusters.
// The response format matches the Ray Dashboard API: summary is a flat array where each element
// is a single node object {hostname, ip, raylet: {...}, ...}. For historical clusters, we use
// the latest state transition snapshot to represent each node's final state.
//
// Frontend references:
//   - summary[i].raylet.state: https://github.com/ray-project/ray/blob/27d3d81d47/python/ray/dashboard/client/src/pages/node/hook/useNodeList.ts#L48-L53
//   - nodeLogicalResources[nodeId] (expects string): https://github.com/ray-project/ray/blob/27d3d81d47/python/ray/dashboard/client/src/pages/node/hook/useNodeList.ts#L48
//   - Logs page node.raylet.state filter: https://github.com/ray-project/ray/blob/60d1469959/python/ray/dashboard/client/src/pages/log/Logs.tsx#L92-L97
func (s *ServerHandler) getNodesSummary(nodeMap map[string]eventtypes.Node, sessionName string, resp *restful.Response) {
	// Build node summary. Use the latest snapshot for each node to match Ray Dashboard API format.
	summary := make([]map[string]any, 0, len(nodeMap))
	// Build node logical resources. Frontend expects { nodeId: "0.0/8.0 CPU\n..." } (string per node).
	nodeLogicalResources := make(map[string]string)

	// Process each node: take the latest snapshot (last state transition) for dashboard compatibility.
	for _, node := range nodeMap {
		nodeSummaryReplay := formatNodeSummaryReplayForResp(node, sessionName)
		if len(nodeSummaryReplay) > 0 {
			summary = append(summary, nodeSummaryReplay[len(nodeSummaryReplay)-1])
		}

		// Find the last non-empty resource string. NODE_DEAD transitions have empty
		// resources, so we search backwards for the last ALIVE transition's resource string.
		nodeResourceReplay := formatNodeResourceReplayForResp(node)
		for i := len(nodeResourceReplay) - 1; i >= 0; i-- {
			if rs, ok := nodeResourceReplay[i]["resourceString"].(string); ok && rs != "" {
				nodeLogicalResources[node.NodeID] = rs
				break
			}
		}
	}

	// Build dashboard API-compatible response.
	response := map[string]any{
		"result": true,
		"msg":    "Node summary fetched.",
		"data": map[string]any{
			"summary":              summary,
			"nodeLogicalResources": nodeLogicalResources,
		},
	}

	data, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal nodes response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(data)
}

// getNodesHostNameList returns a list of hostnames for all alive nodes in historical clusters.
func (s *ServerHandler) getNodesHostNameList(nodeMap map[string]eventtypes.Node, resp *restful.Response) {
	hostNameList := make([]string, 0)

	for _, node := range nodeMap {
		// Only include nodes that are ALIVE (check the latest state transition)
		if len(node.StateTransitions) > 0 {
			lastState := node.StateTransitions[len(node.StateTransitions)-1].State
			if lastState == eventtypes.NODE_ALIVE {
				// Use Hostname if available, otherwise use NodeName or NodeID.
				// TODO: Ray does not export Hostname/NodeName in base events yet.
				// Ref: https://github.com/ray-project/ray/issues/60129
				// Once Ray exports these fields, the hostname will be available.
				// For now, we fallback to NodeID.
				hostname := node.Hostname
				if hostname == "" {
					hostname = node.NodeName
				}
				if hostname == "" {
					hostname = node.NodeID
				}
				hostNameList = append(hostNameList, hostname)
			}
		}
	}

	// Build dashboard API-compatible response.
	response := map[string]any{
		"result": true,
		"msg":    "Node hostname list fetched.",
		"data": map[string]any{
			"hostNameList": hostNameList,
		},
	}

	data, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal nodes hostname list response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(data)
}

// getNode retrieves node details for a specific node in a specific cluster session.
// The API schema of live and dead clusters are different:
//   - Live clusters: returns the current snapshot
//   - Dead clusters: returns the historical replay
func (s *ServerHandler) getNode(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Get the target node ID from the path parameter.
	targetNodeId := req.PathParameter("node_id")
	if targetNodeId == "" {
		resp.WriteErrorString(http.StatusBadRequest, "node_id is required")
		return
	}

	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}
	targetNode, found := snap.Nodes[targetNodeId]
	if !found {
		resp.WriteErrorString(http.StatusNotFound, fmt.Sprintf("node %s not found", targetNodeId))
		return
	}

	nodeSummaryReplay := formatNodeSummaryReplayForResp(targetNode, sessionName)

	// Use the latest snapshot to match Ray Dashboard API format.
	// Frontend expects "detail" to be a single object {hostname, ip, raylet: {...}}, not an array.
	// Ref: https://github.com/ray-project/ray/blob/27d3d81d47/python/ray/dashboard/client/src/pages/node/hook/useNodeDetail.ts#L33-L34
	if len(nodeSummaryReplay) == 0 {
		resp.WriteErrorString(http.StatusNotFound, fmt.Sprintf("node %s has no state transitions yet", targetNodeId))
		return
	}
	detail := nodeSummaryReplay[len(nodeSummaryReplay)-1]

	// Fill actors for this node.
	// Frontend expects actors as {[actorId]: ActorDetail}, not an empty array.
	// Ref: https://github.com/ray-project/ray/blob/8a7b47bc5c/python/ray/dashboard/client/src/pages/node/NodeDetail.tsx#L233
	nodeActors := make(map[string]any)
	for _, actor := range snap.Actors {
		if actor.Address.NodeID == targetNodeId {
			nodeActors[actor.ActorID] = formatActorForResponse(actor)
		}
	}
	if detail != nil {
		detail["actors"] = nodeActors
	}

	// Build dashboard API-compatible response.
	response := map[string]any{
		"result": true,
		"msg":    "Node details fetched.",
		"data": map[string]any{
			"detail": detail,
		},
	}

	data, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal nodes response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(data)
}

func (s *ServerHandler) getEvents(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	// Live cluster: proxy to Ray Dashboard
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	// Check if job_id parameter exists in query string (even if empty)
	// This aligns with Ray Dashboard behavior:
	// - /events (no job_id param) → return all events grouped by job_id
	// - /events?job_id= (empty job_id) → filter by empty string (return empty)
	// - /events?job_id=abc → filter by "abc"
	_, jobIDExists := req.Request.URL.Query()["job_id"]
	jobID := req.QueryParameter("job_id")

	var response map[string]any

	if jobIDExists {
		events := make([]map[string]any, 0)
		for _, logEvent := range snap.LogEventsByJobID[jobID] {
			events = append(events, logEvent.ToAPIResponse())
		}
		response = map[string]any{
			"result": true,
			"msg":    "Job events fetched.",
			"data": map[string]any{
				"jobId":  jobID,
				"events": events,
			},
		}
	} else {
		events := make(map[string][]map[string]any, len(snap.LogEventsByJobID))
		for jobID, logEvents := range snap.LogEventsByJobID {
			eventList := make([]map[string]any, 0, len(logEvents))
			for _, logEvent := range logEvents {
				eventList = append(eventList, logEvent.ToAPIResponse())
			}
			events[jobID] = eventList
		}
		response = map[string]any{
			"result": true,
			"msg":    "All events fetched.",
			"data": map[string]any{
				"events": events,
			},
		}
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal events response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

func (s *ServerHandler) getPrometheusHealth(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	resp.WriteErrorString(http.StatusNotImplemented, "Prometheus health is not yet supported for historical sessions.")
}

func (s *ServerHandler) getGrafanaHealth(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	resp.WriteErrorString(http.StatusNotImplemented, "Grafana health is not yet supported for historical sessions.")
}

func (s *ServerHandler) getJobs(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	jobs := make([]eventtypes.Job, 0, len(snap.Jobs))
	for _, job := range snap.Jobs {
		jobs = append(jobs, job)
	}

	// Formate response to match Ray Dashboard API format
	formattedJobs := make([]any, 0)
	for _, job := range jobs {
		formattedJobs = append(formattedJobs, formatJobForResponse(job))
	}

	response := formattedJobs

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal jobs response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// formatJobForResponse will convert eventtypes.Job to the format expected by Ray Dashboard
func formatJobForResponse(job eventtypes.Job) map[string]any {
	// If SubmissionID is empty, try to get it from metadata
	submissionID := job.SubmissionID
	if submissionID == "" && job.Config.Metadata != nil {
		if metaSubmissionID, ok := job.Config.Metadata["job_submission_id"]; ok {
			submissionID = metaSubmissionID
		}
	}

	// Determine job status. Prefer the latest StatusTransition (most accurate),
	// then fall back to job.Status from the definition event, then infer from State.
	// Frontend uses status for filtering and display:
	// Ref: https://github.com/ray-project/ray/blob/beae3b3f94/python/ray/dashboard/client/src/pages/job/hook/useJobList.ts#L12
	status := string(job.Status)
	if len(job.StatusTransitions) > 0 {
		if latest := string(job.StatusTransitions[len(job.StatusTransitions)-1].Status); latest != "" {
			status = latest
		}
	}
	if status == "" {
		// Only infer RUNNING from CREATED state. JOBFINISHED does not imply SUCCEEDED
		// (the driver may have crashed or been stopped), so we leave it empty rather
		// than showing a misleading status.
		if job.State == eventtypes.CREATED {
			status = string(eventtypes.JOBRUNNING)
		}
	}

	// Infer job type if not set. Ray Dashboard expects "SUBMISSION" or "DRIVER".
	jobType := job.JobType
	if jobType == "" {
		if submissionID != "" {
			jobType = "SUBMISSION"
		} else {
			jobType = "DRIVER"
		}
	}

	result := map[string]any{
		"driver_exit_code":          job.DriverExitCode,
		"driver_node_id":            job.DriverNodeID,
		"driver_agent_http_address": job.DriverAgentHttpAddress,
		"runtime_env":               job.RuntimeEnv,
		"metadata":                  job.Config.Metadata,
		"error_type":                job.ErrorType,
		"message":                   job.Message,
		"entrypoint":                job.EntryPoint,
		"status":                    status,
		"driver_info": map[string]any{
			"id":              job.JobID,
			"node_ip_address": job.DriverNodeIPAddress,
			"pid":             job.DriverPID,
		},
		"job_id":        job.JobID,
		"submission_id": submissionID,
		"type":          jobType,
	}

	if !job.StartTime.IsZero() {
		result["start_time"] = job.StartTime.UnixMilli()
	}

	if !job.EndTime.IsZero() {
		result["end_time"] = job.EndTime.UnixMilli()
	}
	return result
}

func (s *ServerHandler) getJob(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	jobID := req.PathParameter("job_id")

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	job, found := snap.Jobs[jobID]
	if !found {
		responseString := fmt.Sprintf("Job %s does not exist", jobID)
		resp.Write([]byte(responseString))
		return
	}

	respData, err := json.Marshal(formatJobForResponse(job))
	if err != nil {
		logrus.Errorf("Failed to marshal jobs response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

func (s *ServerHandler) getClusterStatus(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)

	format := req.QueryParameter("format")

	var respData []byte
	var err error

	if format == "1" {
		clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
		snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
		if !ok {
			s.handleMissingSnapshot(resp)
			return
		}

		// Build cluster status from debug_state.txt and snapshot data
		clusterLogPathPrefix := s.getClusterLogPathPrefix(req)
		statusString := s.buildFormattedClusterStatus(snap, clusterLogPathPrefix, clusterName, sessionName)

		response := FormattedClusterStatusResponse{
			Result: true,
			Msg:    "Got formatted cluster status.",
			Data: FormattedClusterStatusData{
				ClusterStatus: statusString,
			},
		}
		respData, err = json.Marshal(response)
	} else {
		// TODO(https://github.com/ray-project/kuberay/issues/4381#issuecomment-3771499535)
		// Update when Ray dashboard api supports autoscaler V2 which uses
		// GcsClient.get_cluster_status() for /api/cluster_status.
		// Ray frontend only uses format=1, so returning empty data here is fine for now.
		response := ClusterStatusResponse{
			Result: true,
			Msg:    "Got cluster status.",
			Data:   ClusterStatusData{},
		}
		respData, err = json.Marshal(response)
	}
	if err != nil {
		logrus.Errorf("Failed to marshal cluster status response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.Write(respData)
}

// buildFormattedClusterStatus reconstructs the cluster status from debug_state.txt and pending tasks and actors
func (s *ServerHandler) buildFormattedClusterStatus(snap *eventserver.SessionSnapshot, clusterLogPathPrefix, clusterName, sessionName string) string {
	builder := NewClusterStatusBuilder()
	var nodeIDs []string
	for nodeID := range snap.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	// Fallback: snap.Nodes is populated from parsed node events in event storage.
	// If node events were disabled, failed to upload, or the node crashed early
	// before emitting events, snap.Nodes may be empty even if log files (such as debug_state.txt)
	// exist in the object store. We scan the storage directory under the session to discover
	// node IDs directly so we can still read debug_state.txt.
	if len(nodeIDs) == 0 {
		nodeIDs = clusterlogs.ListSessionNodeDirs(s.reader, clusterLogPathPrefix, sessionName)
	}
	successCount := 0

	for _, nodeID := range nodeIDs {
		debugStatePath := path.Join(clusterlogs.RelLogsDir(sessionName, nodeID), "debug_state.txt")

		reader := s.reader.GetContent(clusterLogPathPrefix, debugStatePath)
		if reader == nil {
			logrus.Debugf("No debug_state.txt found for node %s", nodeID)
			continue
		}

		debugState, err := ParseDebugState(reader)
		if err != nil {
			logrus.Debugf("Failed to parse debug_state.txt for node %s: %v", nodeID, err)
			continue
		}

		builder.AddNodeFromDebugState(debugState)
		successCount++
	}

	if len(nodeIDs) > 0 && successCount == 0 {
		logrus.Debugf("Found %d nodes but failed to parse any debug_state.txt for cluster %s session %s", len(nodeIDs), clusterName, sessionName)
	}

	nodes := snap.Nodes
	tasks := snap.Tasks
	actors := make([]eventtypes.Actor, 0, len(snap.Actors))
	for _, a := range snap.Actors {
		actors = append(actors, a)
	}

	// Use the last timestamp from tasks/actors to represent when the cluster was last active.
	// Fallback to session timestamp if no task/actor timestamps are available.
	// Other options are reading raylet.out or gcs_server.out but the files have 100k+ lines, which is inefficient.
	if ts := GetLastTimestamp(tasks, actors); !ts.IsZero() {
		builder.Timestamp = ts
	} else if ts := ParseSessionTimestamp(sessionName); !ts.IsZero() {
		builder.Timestamp = ts
	}

	builder.AddFailedNodesFromNodes(nodes)
	builder.AddPendingDemandsFromTasks(tasks)
	builder.AddPendingDemandsFromActors(actors)

	return builder.FormatStatus()
}

func (s *ServerHandler) getClusterMetadata(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterLogPathPrefix := s.getClusterLogPathPrefix(req)
	storageKey := utils.EndpointPathToStorageKey("/api/v0/cluster_metadata")
	endpointPath := path.Join(sessionName, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)
	reader := s.reader.GetContent(clusterLogPathPrefix, endpointPath)
	if reader == nil {
		resp.WriteErrorString(http.StatusNotFound, "Cluster metadata not found")
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		logrus.Errorf("Failed to read cluster metadata: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, "Failed to read cluster metadata")
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.Write(data)
}

// getFetchedEndpoint handles the catch-all route registered after all explicit routes.
// Live requests are proxied to the Ray Dashboard. For dead sessions, it replays
// built-in and user-configured endpoints that the collector stored under
// {sessionName}/fetched_endpoints/; known missing endpoints receive a schema-valid
// empty response, while unknown endpoints return 404.
func (s *ServerHandler) getFetchedEndpoint(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterLogPathPrefix := s.getClusterLogPathPrefix(req)

	// Use the full request URI (path + query) for storage key lookup.
	// The collector stores keys using the full endpoint URI it polls (see the
	// collector's poll.go), which may include query params (e.g.,
	// "/api/v0/placement_groups?detail=1&limit=10000").
	// RequestURI() includes query params when present, and equals URL.Path when absent.
	storageKey := utils.EndpointPathToStorageKey(req.Request.URL.RequestURI())
	endpointPath := path.Join(sessionName, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)
	reader := s.reader.GetContent(clusterLogPathPrefix, endpointPath)
	if reader == nil {
		// For known frontend endpoints, return empty but valid JSON responses instead of 404.
		// This prevents the frontend from showing error states for endpoints that may not have been
		// collected (e.g., Serve was not enabled on the cluster).
		if emptyResp := missingSnapshotFallback(req.Request.URL.Path); emptyResp != nil {
			resp.Header().Set("Content-Type", "application/json")
			resp.Write(emptyResp)
			return
		}
		resp.WriteErrorString(http.StatusNotFound, "Endpoint data not found in storage")
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		logrus.Errorf("Failed to read fetched endpoint data for %s: %v", req.Request.URL.Path, err)
		resp.WriteErrorString(http.StatusInternalServerError, "Failed to read endpoint data")
		return
	}

	// Ray 2.56 calls bundles.map() without guarding a missing value. Backfill only
	// bundles; optional stats is already guarded by the frontend.
	trimmedPath := strings.TrimRight(req.Request.URL.Path, "/")
	if trimmedPath == "/api/v0/placement_groups" {
		data = ensurePlacementGroupBundles(data)
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.Write(data)
}

// missingSnapshotFallback returns a schema-valid empty payload for a known frontend
// endpoint when a dead session has no stored snapshot. It returns nil for an unknown
// path so the caller preserves the 404 response.
func missingSnapshotFallback(urlPath string) []byte {
	trimmed := strings.TrimRight(urlPath, "/")
	switch trimmed {
	// "applications" must exist so Object.values() doesn't crash. Missing http_options
	// triggers "Serve not started" warning, matching live cluster behavior.
	// Ref: https://github.com/ray-project/ray/blob/27d3d81d47/python/ray/dashboard/client/src/pages/serve/hook/useServeApplications.ts#L30
	// Ref: https://github.com/ray-project/ray/blob/b775a604ce/python/ray/dashboard/client/src/pages/serve/ServeDeploymentsListPage.tsx#L68
	case "/api/serve/applications":
		data, _ := json.Marshal(map[string]any{
			"applications": map[string]any{},
		})
		return data
	case "/api/v0/placement_groups":
		// Match Ray's empty State API response so the replayed frontend does not
		// receive a 404 when no placement-group snapshot was stored.
		// Ref: https://github.com/ray-project/ray/blob/637fd062205393b9e1929996bfe1d49bd3f8469d/python/ray/dashboard/modules/state/state_head.py#L128-L135
		// Ref: https://github.com/ray-project/ray/blob/637fd062205393b9e1929996bfe1d49bd3f8469d/python/ray/dashboard/routes.py#L176-L213
		// Ref: https://github.com/ray-project/ray/blob/637fd062205393b9e1929996bfe1d49bd3f8469d/python/ray/util/state/common.py#L966-L1003
		data, _ := json.Marshal(map[string]any{
			"result": true,
			"msg":    "",
			"data": map[string]any{
				"result": map[string]any{
					"total":                   0,
					"num_after_truncation":    0,
					"num_filtered":            0,
					"result":                  []any{},
					"partial_failure_warning": "",
					"warnings":                nil,
				},
			},
		})
		return data
	default:
		// No snapshot was stored for this job. Ray may return an empty response after
		// finished dataset stats are evicted, and collection can also fail before a non-empty
		// response is persisted, so absence does not prove the job never used Ray Data.
		// Return the live API's empty shape.
		// Ref: https://github.com/ray-project/ray/blob/ray-2.56.0/python/ray/data/_internal/stats.py#L844-L855
		// Ref: https://github.com/ray-project/ray/blob/ray-2.56.0/python/ray/dashboard/modules/data/data_head.py#L135-L143
		if jobID, ok := strings.CutPrefix(trimmed, "/api/data/datasets/"); ok &&
			jobID != "" && !strings.Contains(jobID, "/") {
			data, _ := json.Marshal(map[string]any{
				"datasets": []any{},
			})
			return data
		}
		return nil
	}
}

// ensurePlacementGroupBundles backfills bundles when absent from a stored placement
// group because Ray 2.56 reads it with an unguarded map(). Optional stats is already
// guarded by the frontend. Serve and Ray Data otherwise replay stored payloads as-is;
// their compatibility fallbacks apply only when no snapshot exists.
// Ref: https://github.com/ray-project/ray/blob/ray-2.56.0/python/ray/dashboard/client/src/components/PlacementGroupTable.tsx#L24-L54
// Ref: https://github.com/ray-project/ray/blob/ray-2.56.0/python/ray/dashboard/client/src/components/PlacementGroupTable.tsx#L183-L231
func ensurePlacementGroupBundles(data []byte) []byte {
	var response map[string]any
	if err := json.Unmarshal(data, &response); err != nil {
		return data
	}

	dataField, ok := response["data"].(map[string]any)
	if !ok {
		return data
	}
	resultField, ok := dataField["result"].(map[string]any)
	if !ok {
		return data
	}
	resultArray, ok := resultField["result"].([]any)
	if !ok {
		return data
	}

	modified := false
	for _, item := range resultArray {
		pg, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if _, exists := pg["bundles"]; !exists {
			pg["bundles"] = []any{}
			modified = true
		}
	}
	if !modified {
		return data
	}

	patched, err := json.Marshal(response)
	if err != nil {
		return data
	}
	return patched
}

// getClusterLogPathPrefix returns the cluster storage prefix:
// e.g., cluster-history/{ownerKind}/{clusterNamespace}/{ownerName}/{clusterName}
func (s *ServerHandler) getClusterLogPathPrefix(req *restful.Request) string {
	clusterName, _ := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace, _ := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	ownerKind, _ := req.Attribute(COOKIE_OWNER_KIND_KEY).(string)
	ownerName, _ := req.Attribute(COOKIE_OWNER_NAME_KEY).(string)
	return clusterlogs.Prefix("", ownerKind, ownerName, clusterNamespace, clusterName)
}

func (s *ServerHandler) getNodeLogs(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}
	nodeID := req.QueryParameter("node_id")
	if nodeID != "" && !fs.ValidPath(nodeID) {
		resp.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("invalid path: path traversal not allowed (node_id=%s)", nodeID))
		return
	}

	var folder, glob string
	if req.QueryParameter("glob") != "" {
		glob = req.QueryParameter("glob")
		// SplitPattern splits e.g. "logs/raylet*" into base="logs" and pattern="raylet*",
		// so we can use base as the storage directory prefix and pattern for matching.
		// For a flat pattern like "raylet*", base is "." which we treat as no subdirectory.
		base, pattern := doublestar.SplitPattern(glob)
		glob = pattern
		if base != "." {
			if !fs.ValidPath(base) {
				resp.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("invalid path: path traversal not allowed (glob=%s)", req.QueryParameter("glob")))
				return
			}
			folder = base
		}
	}
	clusterLogPathPrefix := s.getClusterLogPathPrefix(req)
	data, err := s._getNodeLogs(clusterLogPathPrefix, sessionName, nodeID, folder, glob)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		resp.WriteError(400, err)
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("X-Content-Type-Options", "nosniff")
	resp.Write(data)
}

func (s *ServerHandler) getLogicalActors(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	// Format response to match Ray Dashboard API format
	formattedActors := make(map[string]any, len(snap.Actors))
	for _, actor := range snap.Actors {
		formattedActors[actor.ActorID] = formatActorForResponse(actor)
	}

	response := map[string]any{
		"result": true,
		"msg":    "All actors fetched.",
		"data": map[string]any{
			"actors": formattedActors,
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal actors response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// formatActorForResponse converts an eventtypes.Actor to the format expected by Ray Dashboard.
// Uses camelCase keys and hex-encoded IDs to match the Ray Dashboard API format.
// Ref: ActorDetail type https://github.com/ray-project/ray/blob/a8fdb50e72/python/ray/dashboard/client/src/type/actor.ts#L18-L69
func formatActorForResponse(actor eventtypes.Actor) map[string]any {
	result := map[string]any{
		"actorId": actor.ActorID,
		"jobId":   actor.JobID,
		"state":   string(actor.State),
		"pid":     actor.PID,
		"address": map[string]any{
			"nodeId":    actor.Address.NodeID,
			"ipAddress": actor.Address.IPAddress,
			"port":      actor.Address.Port,
			"workerId":  actor.Address.WorkerID,
		},
		"name":              actor.Name,
		"numRestarts":       strconv.Itoa(actor.NumRestarts),
		"actorClass":        actor.ActorClass,
		"className":         actor.ActorClass,
		"requiredResources": actor.RequiredResources,
		// Note: The key is "exitDetail" (singular), not "exitDetails" (plural). This matches
		// the Ray Dashboard frontend TypeScript type and the live Ray Dashboard API.
		// Ref: https://github.com/ray-project/ray/blob/a8fdb50e72/python/ray/dashboard/client/src/type/actor.ts#L33
		"exitDetail":       actor.ExitDetails,
		"reprName":         actor.ReprName,
		"placementGroupId": actor.PlacementGroupID,
		"callSite":         actor.CallSite,
		"labelSelector":    actor.LabelSelector,
	}

	if !actor.StartTime.IsZero() {
		result["startTime"] = actor.StartTime.UnixMilli()
	}

	if !actor.EndTime.IsZero() {
		result["endTime"] = actor.EndTime.UnixMilli()
		result["timestamp"] = float64(actor.EndTime.UnixMilli())
	}

	return result
}

func (s *ServerHandler) getLogicalActor(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	actorID := req.PathParameter("single_actor")

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	// Actor IDs are normalized to hex at ingestion time, so lookup is by hex ID.
	actor, found := snap.Actors[actorID]

	replyActorInfo := ReplyActorInfo{
		Data: ActorInfoData{},
	}

	if found {
		replyActorInfo.Result = true
		replyActorInfo.Msg = "Actor fetched."
		replyActorInfo.Data.Detail = formatActorForResponse(actor)
	} else {
		replyActorInfo.Result = false
		replyActorInfo.Msg = "Actor not found."
	}

	actData, err := json.MarshalIndent(&replyActorInfo, "", "  ")
	if err != nil {
		logrus.Errorf("Failed to marshal actor response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(actData)
}

func (s *ServerHandler) getNodeLogFile(req *restful.Request, resp *restful.Response) {
	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	// Parse query parameters into GetLogFileOptions struct
	options, err := parseGetLogFileOptions(req)
	if err != nil {
		resp.WriteErrorString(http.StatusBadRequest, err.Error())
		return
	}

	// node_id or node_ip is required when not using actor_id or task_id (they can auto-resolve node_id)
	if options.NodeID == "" && options.NodeIP == "" && options.ActorID == "" && options.TaskID == "" {
		resp.WriteErrorString(http.StatusBadRequest, "node_id or node_ip is required when actor_id or task_id is not provided")
		return
	}

	// Validate required parameters following Ray Dashboard logic
	// At least one of: actor_id, task_id, pid, filename, submission_id must be provided
	if options.ActorID == "" && options.TaskID == "" && options.PID == 0 && options.Filename == "" {
		resp.WriteErrorString(http.StatusBadRequest, "At least one of actor_id, task_id, pid, or filename is required")
		return
	}

	// Prevent path traversal attacks (e.g., ../../etc/passwd)
	if options.NodeID != "" && !fs.ValidPath(options.NodeID) {
		resp.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("invalid path: path traversal not allowed (node_id=%s)", options.NodeID))
		return
	}
	if options.Filename != "" && !fs.ValidPath(options.Filename) {
		resp.WriteErrorString(http.StatusBadRequest, fmt.Sprintf("invalid path: path traversal not allowed (filename=%s)", options.Filename))
		return
	}

	// For live cluster, proxy the request directly to Ray Dashboard without any processing
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterSessionKey := utils.BuildClusterSessionKey(clusterNameID, clusterNamespace, sessionName)
	if _, ok := s.sessionLoader.GetSnapshot(clusterSessionKey); !ok {
		s.handleMissingSnapshot(resp)
		return
	}

	// Only resolve node_ip to node_id from stored events for dead cluster
	clusterLogPathPrefix := s.getClusterLogPathPrefix(req)
	if options.NodeID == "" && options.NodeIP != "" {
		nodeID, err := s.ipToNodeId(clusterLogPathPrefix, sessionName, options.NodeIP)
		if err != nil {
			resp.WriteErrorString(http.StatusNotFound,
				fmt.Sprintf("Cannot find matching node_id for a given node ip %s", options.NodeIP))
			return
		}
		options.NodeID = nodeID
	}

	content, err := s._getNodeLogFile(clusterSessionKey, clusterLogPathPrefix, sessionName, options)
	if err != nil {
		var httpErr *utils.HTTPError
		if errors.As(err, &httpErr) {
			logrus.Errorf("Error getting node log file: %v", httpErr.Unwrap())
			resp.WriteError(httpErr.StatusCode(), httpErr)
		} else {
			logrus.Errorf("Error getting node log file: %v", err)
			resp.WriteError(http.StatusInternalServerError, err)
		}
		return
	}

	// Set Content-Disposition header to trigger file download.
	// If the user provides download_filename, use that; otherwise, use DEFAULT_DOWNLOAD_FILENAME.
	// This matches Ray Dashboard behavior where Content-Disposition is always set.
	if options.DownloadFilename != "" {
		// Format filename correctly to escape " or \
		disposition := mime.FormatMediaType("attachment", map[string]string{
			"filename": options.DownloadFilename,
		})
		if disposition == "" {
			logrus.Errorf("Failed to format Content-Disposition header for filename %q", options.DownloadFilename)
			disposition = fmt.Sprintf("attachment; filename=\"%s\"", DEFAULT_DOWNLOAD_FILENAME)
		}
		resp.AddHeader("Content-Disposition", disposition)
	}

	resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	resp.Header().Set("X-Content-Type-Options", "nosniff")
	resp.Write(content)
}

// parseGetLogFileOptions parses query parameters into GetLogFileOptions struct
func parseGetLogFileOptions(req *restful.Request) (GetLogFileOptions, error) {
	options := GetLogFileOptions{
		NodeID:   req.QueryParameter("node_id"),
		NodeIP:   req.QueryParameter("node_ip"),
		Filename: req.QueryParameter("filename"),
		TaskID:   req.QueryParameter("task_id"),
		ActorID:  req.QueryParameter("actor_id"),
		Suffix:   req.QueryParameter("suffix"),
	}

	// Parse PID parameter
	if pidStr := req.QueryParameter("pid"); pidStr != "" {
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			return options, fmt.Errorf("invalid pid parameter: %s", pidStr)
		}
		options.PID = pid
	}

	// Default suffix to "out" if not specified
	if options.Suffix == "" {
		options.Suffix = "out"
	}

	// Validate suffix parameter
	if options.Suffix != "out" && options.Suffix != "err" {
		return options, fmt.Errorf("invalid suffix parameter: %s (must be 'out' or 'err')", options.Suffix)
	}

	// Parse lines parameter
	if linesStr := req.QueryParameter("lines"); linesStr != "" {
		lines, err := strconv.Atoi(linesStr)
		if err != nil {
			return options, fmt.Errorf("invalid lines parameter: %s", linesStr)
		}
		options.Lines = lines
	}

	// Parse timeout parameter
	if timeoutStr := req.QueryParameter("timeout"); timeoutStr != "" {
		timeout, err := strconv.Atoi(timeoutStr)
		if err != nil {
			return options, fmt.Errorf("invalid timeout parameter: %s", timeoutStr)
		}
		options.Timeout = timeout
	}

	// Parse attempt_number parameter
	if attemptStr := req.QueryParameter("attempt_number"); attemptStr != "" {
		attemptNumber, err := strconv.Atoi(attemptStr)
		if err != nil {
			return options, fmt.Errorf("invalid attempt_number parameter: %s", attemptStr)
		}
		options.AttemptNumber = attemptNumber
	}

	// Parse filter_ansi_code parameter (boolean, default: false)
	if filterStr := req.QueryParameter("filter_ansi_code"); filterStr != "" {
		switch strings.ToLower(filterStr) {
		case "true":
			options.FilterAnsiCode = true
		case "false":
			options.FilterAnsiCode = false
		default:
			return options, fmt.Errorf("invalid filter_ansi_code parameter: %s (must be 'true' or 'false')", filterStr)
		}
	}

	// Parse download_filename parameter
	// If provided, download with the specified filename
	if downloadFilename := req.QueryParameter("download_filename"); downloadFilename != "" {
		options.DownloadFilename = downloadFilename
	} else {
		options.DownloadFilename = DEFAULT_DOWNLOAD_FILENAME
	}

	return options, nil
}

func (s *ServerHandler) getNodeLogStream(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	// Streaming only available for live clusters
	if sessionName != "live" {
		resp.WriteErrorString(http.StatusNotImplemented, "Log streaming only available for live clusters")
		return
	}

	// Forward to Ray Dashboard
	s.redirectRequest(req, resp)
}

// getNodeLog is a unified endpoint that dispatches to getNodeLogFile or getNodeLogStream
// based on the {media_type} path parameter, matching Ray Dashboard's GET /api/v0/logs/{media_type}.
func (s *ServerHandler) getNodeLog(req *restful.Request, resp *restful.Response) {
	mediaType := req.PathParameter("media_type")
	switch mediaType {
	case "file":
		s.getNodeLogFile(req, resp)
	case "stream":
		s.getNodeLogStream(req, resp)
	default:
		resp.WriteErrorString(http.StatusBadRequest,
			fmt.Sprintf("unsupported media_type: %s (must be 'file' or 'stream')", mediaType))
	}
}

func (s *ServerHandler) getTaskSummarize(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	listAPIOptions, err := utils.ParseOptionsFromReq(req)
	if err != nil {
		resp.WriteErrorString(http.StatusBadRequest, err.Error())
		return
	}
	summaryBy := req.QueryParameter("summary_by")

	// For summary, try getting as many entries as possible to minimize data loss.
	// Ref: https://github.com/ray-project/ray/blob/ad1b87448fec4db7ef11f1697f9bc02ae6a7ba09/python/ray/dashboard/state_aggregator.py#L569-L582
	listAPIOptions.Limit = utils.RayMaxLimitFromAPIServer

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}
	tasks := snap.Tasks

	// Calculate the number of tasks after GCS source truncation.
	// Since we can't access the GCS and num_status_task_events_dropped, we use num_after_truncation to approximate the total number of tasks.
	// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/dashboard/state_aggregator.py#L314-L342
	numAfterTruncation := len(tasks)
	numTotal := numAfterTruncation

	// Filter tasks.
	// numFiltered is the number of tasks after filtering but before limit truncation.
	tasks, numFiltered := utils.ApplyTaskFilters(tasks, listAPIOptions)

	// The response envelope follows Ray Dashboard's rest_response format:
	//   { "result": <bool>, "msg": <string>, "data": <payload> }
	// "result" is true when status_code == 200, false otherwise.
	// "msg" is "" on success, or the error message on failure.
	// Error cases (bad request, marshal failure) are handled above via resp.WriteErrorString
	// and return early, so reaching this point means success.
	// Ref: https://github.com/ray-project/ray/blob/master/python/ray/dashboard/routes.py
	var response any
	if summaryBy == "lineage" {
		actors := make([]eventtypes.Actor, 0, len(snap.Actors))
		for _, a := range snap.Actors {
			actors = append(actors, a)
		}
		lineageSummary := utils.ToSummaryByLineage(tasks, actors)

		response = map[string]any{
			"result": true,
			"msg":    "",
			"data": map[string]any{
				"result": map[string]any{
					"total":                numTotal,
					"num_after_truncation": numAfterTruncation,
					"num_filtered":         numFiltered,
					"result": map[string]any{
						"node_id_to_summary": map[string]*utils.TaskSummaries{
							"cluster": lineageSummary,
						},
					},
					"partial_failure_warning": "",
					"warnings":                nil,
				},
			},
		}
	} else {

		funcNameSummary := summarizeTasksByFuncName(tasks)

		response = map[string]any{
			"result": true,
			"msg":    "",
			"data": map[string]any{
				"result": map[string]any{
					"total":                numTotal,
					"num_after_truncation": numAfterTruncation,
					"num_filtered":         numFiltered,
					"result": map[string]any{
						"node_id_to_summary": map[string]*utils.TaskSummariesByFuncName{
							"cluster": funcNameSummary,
						},
					},
					"partial_failure_warning": "",
					"warnings":                nil,
				},
			},
		}
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal task summarize response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// summarizeTasksByFuncName groups tasks by function name and counts by state.
//
// The Ray Dashboard frontend always calls this API with a job_id filter
// (e.g. filter_keys=job_id&filter_predicates=%3D&filter_values={jobId}),
// so the summary only includes tasks belonging to that specific job.
// Internal Ray tasks like _StatsActor (used by Ray Data) belong to a different
// job and are excluded by the filter before reaching this function.
// Ref: https://github.com/ray-project/ray/blob/777f37f002c14bd4c587f4d095b85c62690647de/python/ray/dashboard/client/src/service/job.ts
//
// Ray API source code: https://github.com/ray-project/ray/blob/777f37f002c14bd4c587f4d095b85c62690647de/python/ray/util/state/common.py#L1035-L1064
func summarizeTasksByFuncName(tasks []eventtypes.Task) *utils.TaskSummariesByFuncName {
	summary := make(map[string]*utils.TaskSummaryPerFuncOrClassName)
	totalTasks := 0
	totalActorTasks := 0
	totalActorScheduled := 0

	for _, task := range tasks {
		funcName := task.GetFuncName()
		if funcName == "" {
			// Skip tasks without a function name. This can happen when we've received
			// a TASK_LIFECYCLE_EVENT but not yet the corresponding TASK_DEFINITION_EVENT,
			// since only the definition event carries the FunctionDescriptor (TaskFunc/ActorFunc).
			//
			// Unlike the live Ray Dashboard (which reads complete TaskInfo from GCS in a single object),
			// the history server ingests definition and lifecycle as separate events that may arrive
			// out of order. A task missing its definition also lacks TaskType, so it cannot contribute
			// to totalTasks/totalActorTasks/totalActorScheduled counts either.
			//
			// We skip rather than using a fallback like "unknown" to keep the summary clean for the
			// Dashboard frontend, which does not expect such a synthetic key.
			// Ref: https://github.com/ray-project/ray/blob/777f37f002c14bd4c587f4d095b85c62690647de/python/ray/util/state/common.py#L1049
			continue
		}

		if _, ok := summary[funcName]; !ok {
			summary[funcName] = &utils.TaskSummaryPerFuncOrClassName{
				FuncOrClassName: funcName,
				Type:            string(task.TaskType),
				StateCounts:     make(map[string]int),
			}
		}

		// Use "NIL" for empty state to match Ray's behavior: when a task has no lifecycle events,
		// Ray's protobuf_to_task_state_dict defaults to "NIL" (protobuf TaskStatus enum value 0).
		// Ref: https://github.com/ray-project/ray/blob/777f37f002c14bd4c587f4d095b85c62690647de/python/ray/util/state/common.py#L1688-L1691
		state := string(task.State)
		if state == "" {
			state = "NIL"
		}
		summary[funcName].StateCounts[state]++

		// Count by task type
		switch task.TaskType {
		case eventtypes.NORMAL_TASK:
			totalTasks++
		case eventtypes.ACTOR_CREATION_TASK:
			totalActorScheduled++
		case eventtypes.ACTOR_TASK:
			totalActorTasks++
		}
	}

	return &utils.TaskSummariesByFuncName{
		Summary:             summary,
		TotalTasks:          totalTasks,
		TotalActorTasks:     totalActorTasks,
		TotalActorScheduled: totalActorScheduled,
		SummaryBy:           "func_name",
	}
}

// getTasks handles the /api/v0/tasks endpoint with the task filtering logic by query parameters.
func (s *ServerHandler) getTasks(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Parse query parameters.
	listAPIOptions, err := utils.ParseOptionsFromReq(req)
	if err != nil {
		resp.WriteErrorString(http.StatusBadRequest, err.Error())
		return
	}

	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}
	tasks := snap.Tasks

	// Calculate the number of tasks after GCS source truncation.
	// Since we can't access the GCS and num_status_task_events_dropped, we use num_after_truncation to approximate the total number of tasks.
	// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/dashboard/state_aggregator.py#L314-L342
	numAfterTruncation := len(tasks)
	numTotal := numAfterTruncation

	// Filter tasks.
	// numFiltered is the number of tasks after filtering but before limit truncation.
	tasks, numFiltered := utils.ApplyTaskFilters(tasks, listAPIOptions)

	// Format tasks for response.
	formattedTasks := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		formattedTasks = append(formattedTasks, formatTaskForResponse(task, listAPIOptions.Detail))
	}

	response := RespTasksInfo{
		Result: true,
		Msg:    "",
		Data: TaskData{
			Result: TaskDataResult{
				Total:                 numTotal,
				Result:                formattedTasks,
				NumAfterTruncation:    numAfterTruncation,
				NumFiltered:           numFiltered,
				PartialFailureWarning: "",
				Warnings:              nil,
			},
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal task response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(respData)
}

func formatTaskLogInfoForResponse(taskLogInfo *eventtypes.TaskLogInfo) any {
	if taskLogInfo == nil {
		return nil
	}
	return map[string]any{
		"stdout_file":  taskLogInfo.StdoutFile,
		"stderr_file":  taskLogInfo.StderrFile,
		"stdout_start": taskLogInfo.StdoutStart,
		"stdout_end":   taskLogInfo.StdoutEnd,
		"stderr_start": taskLogInfo.StderrStart,
		"stderr_end":   taskLogInfo.StderrEnd,
	}
}

// formatTaskForResponse formats a task data result of a single task attempt for the response.
// The schema aligns with the Ray Dashboard API.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L730-L819.
func formatTaskForResponse(task eventtypes.Task, detail bool) map[string]any {
	// TODO(jiangjiawei1103): Maybe define result schema in types.go.
	result := map[string]any{
		"task_id":            task.TaskID,
		"attempt_number":     task.TaskAttempt,
		"name":               task.GetTaskName(),
		"state":              string(task.State),
		"job_id":             task.JobID,
		"actor_id":           task.ActorID,
		"type":               string(task.TaskType),
		"func_or_class_name": task.GetFuncName(),
		"parent_task_id":     task.ParentTaskID,
		"node_id":            task.NodeID,
		"worker_id":          task.WorkerID,
		"worker_pid":         task.WorkerPID,
	}
	if task.RayErrorInfo != nil {
		result["error_type"] = string(task.RayErrorInfo.ErrorType)
	} else {
		result["error_type"] = nil
	}

	if detail {
		result["language"] = string(task.Language)
		result["required_resources"] = task.RequiredResources
		// Frontend expects runtime_env_info as a JSON string, not an object.
		// Serialize to match the Ray State API format.
		// Ref: https://github.com/ray-project/ray/blob/2f93603ad1/python/ray/dashboard/client/src/type/task.ts#L39
		runtimeEnvInfoObj := map[string]any{
			"serialized_runtime_env": task.SerializedRuntimeEnv,
			// RuntimeEnvUris and RuntimeEnvConfig are never populated on the Ray side.
			// Ref: https://github.com/ray-project/ray/blob/50c715e79c5ca93118e1280f3842a1946b2cddac/src/ray/core_worker/task_event_buffer.cc#L189-L237.
			"runtime_env_config": map[string]any{
				"setup_timeout_seconds": 600,
				"eager_install":         true,
				"log_files":             []string{},
			},
		}
		runtimeEnvInfoBytes, _ := json.Marshal(runtimeEnvInfoObj)
		result["runtime_env_info"] = string(runtimeEnvInfoBytes)
		isNil, err := utils.IsHexNil(task.PlacementGroupID)
		if isNil || task.PlacementGroupID == "" || err != nil {
			result["placement_group_id"] = nil
		} else {
			result["placement_group_id"] = task.PlacementGroupID
		}

		events := make([]map[string]any, 0, len(task.StateTransitions))
		for _, event := range task.StateTransitions {
			events = append(events, map[string]any{
				"state":      string(event.State),
				"created_ms": event.Timestamp.UnixMilli(),
			})
		}
		result["events"] = events
		// TODO(jiangjiawei1103): Support profiling_data after TASK_PROFILE_EVENT is supported.
		// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1616-L1622.
		// result["profiling_data"] = task.ProfilingData
		result["task_log_info"] = formatTaskLogInfoForResponse(task.TaskLogInfo)
		if task.RayErrorInfo != nil {
			result["error_message"] = task.RayErrorInfo.ErrorMessage
		} else {
			result["error_message"] = nil
		}
		if task.IsDebuggerPaused != nil {
			result["is_debugger_paused"] = *task.IsDebuggerPaused
		} else {
			result["is_debugger_paused"] = nil
		}
		if task.CallSite != nil {
			result["call_site"] = *task.CallSite
		} else {
			result["call_site"] = nil
		}
		if task.LabelSelector != nil {
			result["label_selector"] = task.LabelSelector
		} else {
			result["label_selector"] = map[string]string{}
		}

		if !task.CreationTime.IsZero() {
			result["creation_time_ms"] = task.CreationTime.UnixMilli()
		} else {
			result["creation_time_ms"] = nil
		}
		if !task.StartTime.IsZero() {
			result["start_time_ms"] = task.StartTime.UnixMilli()
		} else {
			result["start_time_ms"] = nil
		}
		if !task.EndTime.IsZero() {
			result["end_time_ms"] = task.EndTime.UnixMilli()
		} else {
			result["end_time_ms"] = nil
		}
	}

	return result
}

// CookieHandle is a preprocessing filter function
func (s *ServerHandler) CookieHandle(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// Get cookie from request
	clusterName, err := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "Cluster Cookie not found")
		return
	}
	sessionName, err := req.Request.Cookie(COOKIE_SESSION_NAME_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "RayCluster Session Name Cookie not found")
		return
	}
	clusterNamespace, err := req.Request.Cookie(COOKIE_CLUSTER_NAMESPACE_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "Cluster Namespace Cookie not found")
		return
	}

	// It is okay for owner to be empty but they should still be available
	ownerKind, err := req.Request.Cookie(COOKIE_OWNER_KIND_KEY)
	if err != nil {
		ownerKind = &http.Cookie{Name: COOKIE_OWNER_KIND_KEY, Value: ""}
	}
	ownerName, err := req.Request.Cookie(COOKIE_OWNER_NAME_KEY)
	if err != nil {
		ownerName = &http.Cookie{Name: COOKIE_OWNER_NAME_KEY, Value: ""}
	}

	// Validate cookie values to prevent path traversal attacks
	if !fs.ValidPath(clusterName.Value) || !fs.ValidPath(clusterNamespace.Value) || !fs.ValidPath(sessionName.Value) {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, fmt.Sprintf("invalid cookie values: path traversal not allowed (cluster_name=%s, cluster_namespace=%s, session_name=%s)", clusterName.Value, clusterNamespace.Value, sessionName.Value))
		return
	}
	if ownerKind.Value != "" {
		kindLower := strings.ToLower(ownerKind.Value)
		if kindLower != utils.RayClusterKind && kindLower != utils.RayJobKind && kindLower != utils.RayServiceKind {
			resp.WriteHeaderAndEntity(http.StatusBadRequest, fmt.Sprintf("invalid cookie values: unsupported owner_kind=%s (must be raycluster, rayjob, or rayservice)", ownerKind.Value))
			return
		}
	}
	if ownerName.Value != "" && !fs.ValidPath(ownerName.Value) {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, fmt.Sprintf("invalid cookie values: path traversal not allowed (owner_name=%s)", ownerName.Value))
		return
	}

	if sessionName.Value == "live" && !s.enableLiveClusters {
		logrus.Warnf("Rejected live session request for %s/%s: live cluster access is disabled", clusterNamespace.Value, clusterName.Value)
		resp.WriteHeaderAndEntity(http.StatusForbidden, "live cluster access is disabled on this History Server")
		return
	}

	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAME_KEY, Value: clusterName.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAMESPACE_KEY, Value: clusterNamespace.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_SESSION_NAME_KEY, Value: sessionName.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_OWNER_KIND_KEY, Value: ownerKind.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_OWNER_NAME_KEY, Value: ownerName.Value})

	if sessionName.Value == "live" {
		// Always query K8s to get the service name to prevent SSRF attacks.
		// Do not trust user-provided cookies for service name.
		// TODO: here might be a bottleneck if there are many requests in the future.
		svcInfo, err := s.clientManager.GetSvcInfo(clusterName.Value, clusterNamespace.Value)
		if err != nil {
			resp.WriteHeaderAndEntity(http.StatusBadRequest, err.Error())
			return
		}
		req.SetAttribute(ATTRIBUTE_SERVICE_NAME, svcInfo)

		// If auth token mode is enabled, fetch the auth token for this cluster
		if s.useAuthTokenMode {
			authToken, err := s.clientManager.GetAuthTokenForRayCluster(req.Request.Context(), clusterNamespace.Value, clusterName.Value)
			if err != nil {
				logrus.Errorf("Failed to get auth token for cluster %s/%s: %v", clusterNamespace.Value, clusterName.Value, err)
				resp.WriteErrorString(
					http.StatusInternalServerError,
					fmt.Sprintf(
						"failed to get auth token for cluster %s/%s: %v",
						clusterNamespace.Value,
						clusterName.Value,
						err,
					),
				)
				return
			}
			req.SetAttribute(ATTRIBUTE_AUTH_TOKEN, authToken)
		}
	}
	req.SetAttribute(COOKIE_CLUSTER_NAME_KEY, clusterName.Value)
	req.SetAttribute(COOKIE_SESSION_NAME_KEY, sessionName.Value)
	req.SetAttribute(COOKIE_CLUSTER_NAMESPACE_KEY, clusterNamespace.Value)
	req.SetAttribute(COOKIE_OWNER_KIND_KEY, ownerKind.Value)
	req.SetAttribute(COOKIE_OWNER_NAME_KEY, ownerName.Value)
	logrus.Infof("Request URL %s", req.Request.URL.String())
	chain.ProcessFilter(req, resp)
}

// fetchSvcInfo retrieves the RayCluster and derives the head service routing info.
func (c *ClientManager) fetchSvcInfo(name, namespace string) (ServiceInfo, error) {
	if len(c.clients) == 0 {
		return ServiceInfo{}, errors.New("No available kubernetes config found")
	}

	rc, err := c.GetRayCluster(context.Background(), namespace, name)
	if err != nil {
		return ServiceInfo{}, err
	}
	svcName := rc.Status.Head.ServiceName
	if svcName == "" {
		return ServiceInfo{}, errors.New("RayCluster head service not ready")
	}

	return ServiceInfo{
		ServiceName: svcName,
		Namespace:   namespace,
		Port:        getDashboardPort(&rc.Spec),
	}, nil
}

// getDashboardPort resolves the dashboard port the ray-operator assigns to the head service,
// which mirrors the dashboard port declared on the head container.
//
// Only the dashboard port is resolved here, because that is the only port the history server
// proxies to. If other head service ports (e.g. client, serve, metrics) are needed later, copy
// the relevant port-resolution code from the ray-operator into this package rather than
// importing it, so the history server stays independent of the operator's internal packages:
// https://github.com/ray-project/kuberay/blob/11de28835b73585091190b9b08c0d8bd5b84f779/ray-operator/controllers/ray/common/service.go#L402-L444
func getDashboardPort(spec *rayv1.RayClusterSpec) int {
	containers := spec.HeadGroupSpec.Template.Spec.Containers
	if len(containers) > utils.RayContainerIndex {
		for _, p := range containers[utils.RayContainerIndex].Ports {
			if p.Name == utils.DashboardPortName {
				return int(p.ContainerPort)
			}
		}
	}
	return utils.DefaultDashboardPort
}

// formatNodeSummaryReplayForResp formats a node summary replay of a single node for the response.
// Fields must match the Ray Dashboard frontend NodeDetail type to avoid TypeError crashes.
// Ref: NodeDetail.tsx (loadAvg, networkSpeed, cmdline, disk, workers, actors):
//
//	https://github.com/ray-project/ray/blob/8a7b47bc5c/python/ray/dashboard/client/src/pages/node/NodeDetail.tsx#L65-L233
func formatNodeSummaryReplayForResp(node eventtypes.Node, sessionName string) []map[string]any {
	nodeId := node.NodeID
	nodeIpAddress := node.NodeIPAddress
	labels := node.Labels
	var nodeTypeName string
	if nodeGroup, exists := labels["ray.io/node-group"]; exists {
		nodeTypeName = nodeGroup
	}
	isHeadNode := nodeTypeName == "headgroup"
	rayletSocketName := path.Join(utils.GetTmpRayRoot(), sessionName, "sockets", "raylet")
	objectStoreSocketName := path.Join(utils.GetTmpRayRoot(), sessionName, "sockets", "plasma_store")

	// Handle the start timestamp of the node.
	// Ref: https://github.com/ray-project/ray/blob/f953f199b5d68d47c07c865c5ebcd2333d49f365/src/ray/protobuf/gcs.proto#L345-L346.
	var startTimestamp int64
	if !node.StartTimestamp.IsZero() {
		startTimestamp = node.StartTimestamp.UnixMilli()
	}

	// Wait for Ray to export the following fields.
	// Ref: https://github.com/ray-project/ray/issues/60129
	hostname := node.Hostname
	nodeName := node.NodeName
	instanceID := node.InstanceID
	instanceTypeName := node.InstanceTypeName

	nodeSummaryReplay := make([]map[string]any, 0)
	for _, tr := range node.StateTransitions {
		transitionTimestamp := tr.Timestamp.UnixMilli()
		resourcesTotal := convertResourcesToAPISchema(tr.Resources)

		// Handle DEAD state-specific fields.
		var endTimestamp int64
		var stateMessage string
		if tr.State == eventtypes.NODE_DEAD {
			endTimestamp = tr.Timestamp.UnixMilli()
			if tr.DeathInfo != nil {
				stateMessage = composeStateMessage(string(tr.DeathInfo.Reason), tr.DeathInfo.ReasonMessage)
			}
		}

		// Host-level metrics (cpus, mem, shm, bootTime, disk, gpus, tpus) are not available
		// from Ray Base Events. These metrics can be obtained from Prometheus/Grafana when
		// Ray metrics are enabled. For historical replay, we use placeholder values.
		// Format must match the Ray Dashboard API schema expected by the frontend.
		nodeSummarySnapshot := map[string]any{
			"t":            transitionTimestamp,
			"now":          transitionTimestamp,
			"hostname":     hostname,
			"ip":           nodeIpAddress,
			"cpu":          0,
			"cmdline":      []string{},
			"cpus":         []int{0, 0},
			"mem":          []int{0, 0, 0, 0},
			"shm":          0,
			"bootTime":     0,
			"loadAvg":      [][]float64{{0, 0, 0}, {0, 0, 0}},
			"networkSpeed": []float64{0, 0},
			// Frontend expects disk as {mountPoint: {total, used, free, percent}}, not an array.
			"disk": map[string]map[string]any{
				"/":    {"total": 0, "used": 0, "free": 0, "percent": 0.0},
				"/tmp": {"total": 0, "used": 0, "free": 0, "percent": 0.0},
			},
			// Frontend expects gpus as GPUStats[] ({uuid, name, utilizationGpu, ...}), not int array.
			"gpus": []any{},
			"tpus": []any{},
			// Frontend expects workers as Worker[] and actors as {[actorId]: ActorDetail}.
			// actors is set as empty map here; getNode() fills it with actual data.
			"workers": []any{},
			"actors":  map[string]any{},
			"raylet": map[string]any{
				"storeStats": map[string]any{
					"objectStoreBytesAvail": resourcesTotal["objectStoreMemory"],
				},
				"nodeId":                nodeId,
				"nodeManagerAddress":    nodeIpAddress,
				"nodeManagerHostname":   hostname,
				"rayletSocketName":      rayletSocketName,
				"objectStoreSocketName": objectStoreSocketName,
				"metricsExportPort":     "8080",
				"resourcesTotal":        resourcesTotal,
				"nodeName":              nodeName,
				"instanceId":            instanceID,
				"nodeTypeName":          nodeTypeName,
				"instanceTypeName":      instanceTypeName,
				"startTimeMs":           startTimestamp,
				"isHeadNode":            isHeadNode,
				"labels":                labels,
				"state":                 string(tr.State),
				"endTimeMs":             endTimestamp,
				"stateMessage":          stateMessage,
			},
		}
		nodeSummaryReplay = append(nodeSummaryReplay, nodeSummarySnapshot)
	}

	return nodeSummaryReplay
}

// formatNodeResourceReplayForResp formats a node resource replay of a single node for the response.
func formatNodeResourceReplayForResp(node eventtypes.Node) []map[string]any {
	nodeResourceReplay := make([]map[string]any, 0)
	for _, tr := range node.StateTransitions {
		transitionTimestamp := tr.Timestamp.UnixMilli()

		// Create a resource snapshot.
		var resourceString string
		if tr.State == eventtypes.NODE_ALIVE {
			resourceString = constructResourceString(tr.Resources)
		}
		nodeResourceSnapshot := map[string]any{
			"t":              transitionTimestamp,
			"resourceString": resourceString,
		}
		nodeResourceReplay = append(nodeResourceReplay, nodeResourceSnapshot)
	}

	return nodeResourceReplay
}

// convertResourcesToAPISchema converts Ray's resource format to Dashboard API schema.
// Conversion rules:
//   - "object_store_memory" is converted to "objectStoreMemory"
//   - "node:__internal_head__" is converted to "node:InternalHead"
//   - Other fields remain unchanged (e.g., "memory", "CPU", "node:<node-ip>")
func convertResourcesToAPISchema(resources map[string]float64) map[string]float64 {
	if len(resources) == 0 {
		return map[string]float64{}
	}

	convertedResources := make(map[string]float64, len(resources))
	for k, v := range resources {
		convertedKey := k
		switch k {
		case "object_store_memory":
			convertedKey = "objectStoreMemory"
		case "node:__internal_head__":
			convertedKey = "node:InternalHead"
		}
		convertedResources[convertedKey] = v
	}

	return convertedResources
}

// composeStateMessage composes a state message based on the death reason and message for a node state transition in DEAD state.
// Ref: https://github.com/ray-project/ray/blob/f953f199b5d68d47c07c865c5ebcd2333d49f365/python/ray/dashboard/utils.py#L738-L765.
func composeStateMessage(deathReason string, deathReasonMessage string) string {
	var stateMessage string
	if deathReason == string(eventtypes.EXPECTED_TERMINATION) {
		stateMessage = "Expected termination"
	} else if deathReason == string(eventtypes.UNEXPECTED_TERMINATION) {
		stateMessage = "Unexpected termination"
	} else if deathReason == string(eventtypes.AUTOSCALER_DRAIN_PREEMPTED) {
		stateMessage = "Terminated due to preemption"
	} else if deathReason == string(eventtypes.AUTOSCALER_DRAIN_IDLE) {
		stateMessage = "Terminated due to idle (no Ray activity)"
	} else {
		stateMessage = ""
	}

	if deathReasonMessage != "" {
		if stateMessage != "" {
			stateMessage = fmt.Sprintf("%s: %s", stateMessage, deathReasonMessage)
		} else {
			stateMessage = deathReasonMessage
		}
	}
	return stateMessage
}

// constructResourceString constructs a resource string based on the resources in state transition.
// Note that we skip processing the placement group.
// Ref: https://github.com/ray-project/ray/blob/f953f199b5d68d47c07c865c5ebcd2333d49f365/python/ray/autoscaler/_private/util.py#L643-L665.
func constructResourceString(resources map[string]float64) string {
	resourceKeys := make([]string, 0, len(resources))
	for k := range resources {
		resourceKeys = append(resourceKeys, k)
	}
	sort.Strings(resourceKeys)

	resourceString := ""
	for _, k := range resourceKeys {
		v := resources[k]

		if k == "memory" || k == "object_store_memory" {
			formattedUsed := "0B"
			formattedTotal := formatMemory(v)
			resourceString += fmt.Sprintf("%s/%s %s", formattedUsed, formattedTotal, k)
		} else if strings.HasPrefix(k, "node:") {
			// Skip per-node resources
			continue
		} else if strings.HasPrefix(k, "accelerator_type:") {
			// Skip accelerator_type
			// Ref: https://github.com/ray-project/ray/issues/33272
			continue
		} else {
			// Handle CPU, GPU, TPU, and other resources
			resourceString += fmt.Sprintf("%.1f/%.1f %s", 0.0, v, k)
		}

		resourceString += "\n"
	}
	resourceString = strings.TrimSuffix(resourceString, "\n")

	return resourceString
}

// formatMemory formats a memory value to a human-readable string.
func formatMemory(memBytes float64) string {
	type unit struct {
		suffix       string
		bytesPerUnit float64
	}
	units := []unit{
		{suffix: "TiB", bytesPerUnit: math.Pow(2, 40)},
		{suffix: "GiB", bytesPerUnit: math.Pow(2, 30)},
		{suffix: "MiB", bytesPerUnit: math.Pow(2, 20)},
		{suffix: "KiB", bytesPerUnit: math.Pow(2, 10)},
	}
	for _, unit := range units {
		if memBytes >= unit.bytesPerUnit {
			memInUnit := memBytes / unit.bytesPerUnit
			return fmt.Sprintf("%.2f%s", memInUnit, unit.suffix)
		}
	}
	return fmt.Sprintf("%dB", int(memBytes))
}

func (s *ServerHandler) getTasksTimeline(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	jobID := req.QueryParameter("job_id")
	download := req.QueryParameter("download")

	clusterSessionKey := utils.BuildClusterSessionKey(clusterName, clusterNamespace, sessionName)
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		s.handleMissingSnapshot(resp)
		return
	}
	timeline := getTasksTimeline(snap, jobID)

	respData, err := json.Marshal(timeline)
	if err != nil {
		logrus.Errorf("Failed to marshal timeline response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Support download parameter
	if download == "1" {
		nowStr := time.Now().Format("2006-01-02_15-04-05")
		filename := fmt.Sprintf("timeline-%s-%s.json", jobID, nowStr)
		if jobID == "" {
			filename = fmt.Sprintf("timeline-%s.json", nowStr)
		}
		resp.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	}
	resp.Write(respData)
}
