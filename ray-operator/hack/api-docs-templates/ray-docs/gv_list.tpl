{{- define "gvList" -}}
{{- $groupVersions := . -}}
{{- range $groupVersions }}
{{- template "gvDetails" . }}
{{- end }}
{{ end -}}
