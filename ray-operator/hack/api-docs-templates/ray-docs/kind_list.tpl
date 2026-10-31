{{- /* Renders kinds as an Oxford-comma list, wrapping each in backticks when .code is true. */ -}}
{{- define "kindList" -}}
{{- $n := len .kinds -}}
{{- $code := .code -}}
{{- range $i, $k := .kinds -}}
{{- if gt $i 0 }}{{ if eq $n 2 }} and {{ else if eq (add1 $i) $n }}, and {{ else }}, {{ end }}{{ end -}}
{{- if $code }}`{{ $k }}`{{ else }}{{ $k }}{{ end -}}
{{- end -}}
{{- end -}}
