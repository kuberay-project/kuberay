package html

import _ "embed"

// The cluster selection page and the files it loads.

//go:embed select_cluster.html
var ClusterSelectorHTML []byte

//go:embed select_cluster.css
var ClusterSelectorCSS []byte

//go:embed select_cluster.js
var ClusterSelectorJS []byte

// Roboto as the Ray dashboard bundles it, so the page looks the same without it installed.

//go:embed fonts/roboto-latin-400-normal.woff2
var ClusterSelectorRoboto400 []byte

//go:embed fonts/roboto-latin-500-normal.woff2
var ClusterSelectorRoboto500 []byte
