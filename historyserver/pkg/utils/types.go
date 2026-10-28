package utils

type ClusterInfo struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	SessionName     string `json:"sessionName"`
	CreateTime      string `json:"createTime"`
	CreateTimeStamp int64  `json:"createTimeStamp"`
	OwnerKind       string `json:"ownerKind,omitempty"`
	OwnerName       string `json:"ownerName,omitempty"`
}

type ClusterInfoList []ClusterInfo

func (a ClusterInfoList) Len() int           { return len(a) }
func (a ClusterInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ClusterInfoList) Less(i, j int) bool { return a[i].CreateTimeStamp > a[j].CreateTimeStamp } // Sort descending
