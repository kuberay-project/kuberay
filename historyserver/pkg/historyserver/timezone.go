package historyserver

import (
	"io"
	"net/http"
	"path"

	"github.com/emicklei/go-restful/v3"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const timezoneEndpoint = "/timezone"

func (s *ServerHandler) getTimezone(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterLogPathPrefix := s.getClusterLogPathPrefix(req)

	storageKey := utils.EndpointPathToStorageKey(timezoneEndpoint)
	endpointPath := path.Join(sessionName, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)
	reader := s.reader.GetContent(clusterLogPathPrefix, endpointPath)
	if reader == nil {
		resp.Header().Set("Content-Type", "application/json")
		resp.Write([]byte(`{"offset":"","value":""}`))
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		logrus.Errorf("Failed to read timezone metadata: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, "Failed to read timezone metadata")
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.Write(data)
}
